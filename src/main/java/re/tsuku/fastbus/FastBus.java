package re.tsuku.fastbus;

import java.lang.reflect.Field;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * main event bus for subscribing listeners and posting events.
 *
 * <p>
 * listeners can be registered directly with {@link #subscribe(Class, Listener)} or discovered from an object with
 * {@link Subscribe} methods and fields using {@link #subscribe(Object)}.
 * </p>
 *
 * <p>
 * subscriptions are indexed by event type. posting uses cached immutable listener snapshots for the concrete event
 * class, so dispatch stays on the matching listener set.
 * </p>
 */
public final class FastBus {
    private static final Handler[] EMPTY_HANDLERS = new Handler[0];
    private static final Listener<Event>[] EMPTY_LISTENERS = listenerArray(0);
    private static final ClassValue<List<HandlerFactory>> SUBSCRIBER_FACTORIES = new ClassValue<List<HandlerFactory>>() {
        @Override
        protected List<HandlerFactory> computeValue(Class<?> type) {
            return findSubscriberFactories(type);
        }
    };
    private static final ClassValue<Class<?>[]> DISPATCH_TYPES = new ClassValue<Class<?>[]>() {
        @Override
        protected Class<?>[] computeValue(Class<?> type) {
            return findDispatchTypes(type);
        }
    };

    private final Map<Class<?>, HandlerBucket> handlers = new ConcurrentHashMap<>();
    private final Map<Object, List<Handler>> owners = new IdentityHashMap<>();
    private final AtomicLong sequences = new AtomicLong();
    private volatile long version;
    private final ClassValue<DispatchSlot> dispatchSlots = new ClassValue<DispatchSlot>() {
        @Override
        protected DispatchSlot computeValue(Class<?> type) {
            return new DispatchSlot(type);
        }
    };

    public void subscribe(Object owner) {
        if (owner == null) {
            throw new NullPointerException("owner");
        }

        synchronized (owners) {
            if (owners.containsKey(owner)) {
                return;
            }

            List<Handler> found = scan(owner);
            if (found.isEmpty()) {
                return;
            }

            for (Handler handler : found) {
                addHandler(handler);
            }
            owners.put(owner, found);
            changeVersion();
        }
    }

    public <T extends Event> Subscription subscribe(Class<T> eventType, Listener<? super T> listener) {
        return subscribe(eventType, listener, EventPriority.NORMAL);
    }

    public <T extends Event> Subscription subscribe(Class<T> eventType, Listener<? super T> listener, int priority) {
        if (eventType == null) {
            throw new NullPointerException("eventType");
        }
        if (listener == null) {
            throw new NullPointerException("listener");
        }

        Handler handler = new Handler(eventType, createListenerInvoker(listener), priority, nextSequence());
        addHandler(handler);
        changeVersion();
        return () -> unsubscribe(handler);
    }

    public void unsubscribe(Object owner) {
        if (owner == null) {
            return;
        }

        synchronized (owners) {
            List<Handler> ownedHandlers = owners.remove(owner);
            if (ownedHandlers == null) {
                return;
            }

            for (Handler handler : ownedHandlers) {
                removeHandler(handler);
            }
            changeVersion();
        }
    }

    public boolean subscribed(Object owner) {
        synchronized (owners) {
            return owners.containsKey(owner);
        }
    }

    public <T extends Event> T post(T event) {
        if (event == null) {
            throw new NullPointerException("event");
        }

        Listener<Event>[] snapshot = dispatchSlots.get(event.getClass()).snapshot(version, this);
        for (Listener<Event> listener : snapshot) {
            listener.call(event);
        }
        return event;
    }

    private List<Handler> scan(Object owner) {
        List<Handler> found = new ArrayList<>();
        for (HandlerFactory factory : SUBSCRIBER_FACTORIES.get(owner.getClass())) {
            found.add(factory.create(this, owner));
        }
        return found;
    }

    private static List<HandlerFactory> findSubscriberFactories(Class<?> ownerType) {
        List<HandlerFactory> found = new ArrayList<>();
        Class<?> type = ownerType;
        while (type != null && type != Object.class) {
            for (Method method : type.getDeclaredMethods()) {
                Subscribe subscribe = method.getAnnotation(Subscribe.class);
                if (subscribe == null) {
                    continue;
                }
                found.add(createMethodFactory(method, subscribe.value()));
            }

            for (Field field : type.getDeclaredFields()) {
                Subscribe subscribe = field.getAnnotation(Subscribe.class);
                if (subscribe == null) {
                    continue;
                }
                found.add(createFieldFactory(field, subscribe.value()));
            }

            type = type.getSuperclass();
        }

        return Collections.unmodifiableList(found);
    }

    private static HandlerFactory createMethodFactory(Method method, int priority) {
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("@Subscribe method must have exactly one parameter: " + method);
        }

        Class<?> rawEventType = method.getParameterTypes()[0];
        if (!Event.class.isAssignableFrom(rawEventType)) {
            throw new IllegalArgumentException("@Subscribe method parameter must implement event: " + method);
        }
        Class<? extends Event> eventType = rawEventType.asSubclass(Event.class);

        method.setAccessible(true);
        try {
            MethodHandles.Lookup lookup = lookup(method.getDeclaringClass());
            MethodHandle handle = lookup.unreflect(method);
            MethodInvokerFactory invokerFactory = createMethodInvokerFactory(method, lookup, handle);
            if (throwsCheckedException(method)) {
                return (bus, owner) -> new Handler(eventType, wrapChecked(method, invokerFactory.create(owner)),
                        priority, bus.nextSequence());
            }
            return (bus, owner) -> new Handler(eventType, invokerFactory.create(owner), priority,
                    bus.nextSequence());
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("unable to access listener method: " + method, exception);
        }
    }

    private static MethodHandles.Lookup lookup(Class<?> type) {
        MethodHandles.Lookup lookup = privateLookup(type);
        if (lookup != null) {
            return lookup;
        }
        return java8Lookup(type);
    }

    private static MethodHandles.Lookup privateLookup(Class<?> type) {
        try {
            Method method = MethodHandles.class.getMethod(
                    "privateLookupIn",
                    Class.class,
                    MethodHandles.Lookup.class);
            return (MethodHandles.Lookup) method.invoke(null, type, MethodHandles.lookup());
        } catch (ReflectiveOperationException exception) {
            return null;
        }
    }

    private static MethodHandles.Lookup java8Lookup(Class<?> type) {
        try {
            Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(
                    Class.class,
                    int.class);
            constructor.setAccessible(true);
            int modes = MethodHandles.Lookup.PUBLIC
                    | MethodHandles.Lookup.PRIVATE
                    | MethodHandles.Lookup.PROTECTED
                    | MethodHandles.Lookup.PACKAGE;
            return constructor.newInstance(type, modes);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("unable to create listener lookup: " + type.getName(), exception);
        }
    }

    private static HandlerFactory createFieldFactory(Field field, int priority) {
        if (!Listener.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("@Subscribe field must be a listener: " + field);
        }

        Class<? extends Event> eventType = listenerEventType(field.getGenericType(), field);
        field.setAccessible(true);

        return (bus, owner) -> createFieldHandler(bus, owner, field, eventType, priority);
    }

    private static Handler createFieldHandler(FastBus bus, Object owner, Field field, Class<? extends Event> eventType,
            int priority) {
        try {
            Listener<? extends Event> listener = (Listener<? extends Event>) field.get(owner);
            if (listener == null) {
                throw new IllegalArgumentException("@Subscribe listener field is null: " + field);
            }
            return bus.createListenerHandler(eventType, listener, priority);
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("unable to read listener field: " + field, exception);
        }
    }

    private <T extends Event> Handler createListenerHandler(Class<T> eventType, Listener<? extends Event> listener,
            int priority) {
        @SuppressWarnings("unchecked")
        Listener<? super T> typedListener = (Listener<? super T>) listener;
        return new Handler(eventType, createListenerInvoker(typedListener), priority, nextSequence());
    }

    @SuppressWarnings("unchecked")
    private <T extends Event> Listener<Event> createListenerInvoker(Listener<? super T> listener) {
        return event -> listener.call((T) event);
    }

    private static Class<? extends Event> listenerEventType(Type type, Field field) {
        if (!(type instanceof ParameterizedType)) {
            throw new IllegalArgumentException("@Subscribe listener field must declare its event type: " + field);
        }

        Type eventType = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (!(eventType instanceof Class<?>) || !Event.class.isAssignableFrom((Class<?>) eventType)) {
            throw new IllegalArgumentException("@Subscribe listener field type must implement event: " + field);
        }

        return ((Class<?>) eventType).asSubclass(Event.class);
    }

    private static MethodInvokerFactory createMethodInvokerFactory(Method method, MethodHandles.Lookup lookup,
            MethodHandle handle) {
        try {
            CallSite site = LambdaMetafactory.metafactory(
                    lookup,
                    "call",
                    MethodType.methodType(Listener.class, method.getDeclaringClass()),
                    MethodType.methodType(void.class, Event.class),
                    handle,
                    MethodType.methodType(void.class, method.getParameterTypes()[0]));
            MethodHandle factory = site.getTarget().asType(MethodType.methodType(Listener.class, Object.class));
            return owner -> bindMethodInvoker(factory, owner);
        } catch (Throwable throwable) {
            throw new IllegalStateException("unable to create listener method invoker: " + method, throwable);
        }
    }

    private static Listener<Event> bindMethodInvoker(MethodHandle factory, Object owner) {
        try {
            @SuppressWarnings("unchecked")
            Listener<Event> listener = (Listener<Event>) factory.invokeExact(owner);
            return listener;
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Error error) {
            throw error;
        } catch (Throwable throwable) {
            throw new IllegalStateException("unable to bind listener method", throwable);
        }
    }

    private static Listener<Event> wrapChecked(Method method, Listener<Event> listener) {
        return event -> invokeChecked(method, listener, event);
    }

    private static boolean throwsCheckedException(Method method) {
        for (Class<?> exceptionType : method.getExceptionTypes()) {
            if (!RuntimeException.class.isAssignableFrom(exceptionType)
                    && !Error.class.isAssignableFrom(exceptionType)) {
                return true;
            }
        }
        return false;
    }

    private static void invokeChecked(Method method, Listener<Event> listener, Event event) {
        try {
            listener.call(event);
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Error error) {
            throw error;
        } catch (Throwable throwable) {
            throw new EventDispatchException("listener failed: " + method, throwable);
        }
    }

    private void addHandler(Handler handler) {
        HandlerBucket bucket = handlers.computeIfAbsent(handler.eventType, ignored -> new HandlerBucket());
        synchronized (bucket) {
            bucket.add(handler);
        }
    }

    private void removeHandler(Handler handler) {
        HandlerBucket bucket = handlers.get(handler.eventType);
        if (bucket == null) {
            return;
        }
        synchronized (bucket) {
            bucket.remove(handler);
            if (bucket.empty()) {
                handlers.remove(handler.eventType, bucket);
            }
        }
    }

    private Listener<Event>[] createDispatchSnapshot(Class<?> eventType) {
        List<Handler> matching = new ArrayList<>();
        for (Class<?> dispatchType : DISPATCH_TYPES.get(eventType)) {
            HandlerBucket bucket = handlers.get(dispatchType);
            if (bucket != null) {
                synchronized (bucket) {
                    bucket.addTo(matching);
                }
            }
        }
        if (matching.isEmpty()) {
            return EMPTY_LISTENERS;
        }
        Collections.sort(matching, Handler.ORDER);
        Listener<Event>[] snapshot = listenerArray(matching.size());
        for (int index = 0, limit = snapshot.length; index < limit; index++) {
            snapshot[index] = matching.get(index).listener;
        }
        return snapshot;
    }

    private static Class<?>[] findDispatchTypes(Class<?> eventType) {
        Set<Class<?>> types = new LinkedHashSet<>();
        Class<?> current = eventType;
        while (current != null && current != Object.class) {
            if (Event.class.isAssignableFrom(current)) {
                types.add(current);
            }
            collectEventInterfaces(current, types);
            current = current.getSuperclass();
        }
        types.add(Event.class);
        return types.toArray(new Class<?>[0]);
    }

    private static void collectEventInterfaces(Class<?> type, Set<Class<?>> types) {
        for (Class<?> interfaceType : type.getInterfaces()) {
            if (Event.class.isAssignableFrom(interfaceType)) {
                types.add(interfaceType);
            }
            collectEventInterfaces(interfaceType, types);
        }
    }

    private void unsubscribe(Handler handler) {
        removeHandler(handler);
        changeVersion();
    }

    private long nextSequence() {
        return sequences.getAndIncrement();
    }

    private void changeVersion() {
        version++;
    }

    @FunctionalInterface
    private interface MethodInvokerFactory {
        Listener<Event> create(Object owner);
    }

    @FunctionalInterface
    private interface HandlerFactory {
        Handler create(FastBus bus, Object owner);
    }

    private static final class Handler {
        private static final Comparator<Handler> ORDER = new Comparator<Handler>() {
            @Override
            public int compare(Handler left, Handler right) {
                int priorityOrder = Integer.compare(left.priority, right.priority);
                if (priorityOrder != 0) {
                    return priorityOrder;
                }
                return Long.compare(left.sequence, right.sequence);
            }
        };

        private final Class<? extends Event> eventType;
        private final Listener<Event> listener;
        private final int priority;
        private final long sequence;

        private Handler(Class<? extends Event> eventType, Listener<Event> listener, int priority, long sequence) {
            this.eventType = eventType;
            this.listener = listener;
            this.priority = priority;
            this.sequence = sequence;
        }
    }

    private static final class HandlerBucket {
        private Handler[] handlers = EMPTY_HANDLERS;
        private int size;

        private void add(Handler handler) {
            ensureCapacity(size + 1);

            int priority = handler.priority;
            long sequence = handler.sequence;
            int index = size;

            while (index > 0) {
                int previous = index - 1;
                Handler moved = handlers[previous];
                if (priority > moved.priority) {
                    break;
                }
                if (priority == moved.priority && sequence > moved.sequence) {
                    break;
                }

                handlers[index] = moved;
                index--;
            }

            handlers[index] = handler;
            size++;
        }

        private void remove(Handler handler) {
            int index = 0;
            int limit = size;
            while (index < limit && handlers[index] != handler) {
                index++;
            }
            if (index == limit) {
                return;
            }

            int last = size - 1;
            while (index < last) {
                int next = index + 1;
                handlers[index] = handlers[next];
                index = next;
            }

            handlers[last] = null;
            size = last;
        }

        private void addTo(List<Handler> target) {
            for (int index = 0, limit = size; index < limit; index++) {
                target.add(handlers[index]);
            }
        }

        private boolean empty() {
            return size == 0;
        }

        private void ensureCapacity(int required) {
            if (handlers.length >= required) {
                return;
            }

            int capacity = handlers.length == 0 ? 4 : handlers.length << 1;
            while (capacity < required) {
                capacity <<= 1;
            }
            handlers = Arrays.copyOf(handlers, capacity);
        }
    }

    private static final class DispatchSlot {
        private final Class<?> eventType;
        private volatile Listener<Event>[] snapshot = EMPTY_LISTENERS;
        private volatile long version = -1;

        private DispatchSlot(Class<?> eventType) {
            this.eventType = eventType;
        }

        private Listener<Event>[] snapshot(long currentVersion, FastBus bus) {
            Listener<Event>[] current = snapshot;
            if (version == currentVersion) {
                return current;
            }
            return update(currentVersion, bus);
        }

        private synchronized Listener<Event>[] update(long currentVersion, FastBus bus) {
            if (version != currentVersion) {
                snapshot = bus.createDispatchSnapshot(eventType);
                version = currentVersion;
            }
            return snapshot;
        }
    }

    @SuppressWarnings("unchecked")
    private static Listener<Event>[] listenerArray(int capacity) {
        return (Listener<Event>[]) new Listener<?>[capacity];
    }
}
