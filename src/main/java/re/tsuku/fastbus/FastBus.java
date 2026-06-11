package re.tsuku.fastbus;

import java.lang.reflect.Field;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a small typed event bus.
 *
 * <p>subscriptions are indexed by event type. posting uses a cached immutable
 * handler snapshot for the concrete event class, so dispatch does not scan
 * unrelated listeners.</p>
 */
public final class FastBus {
    private static final Handler[] EMPTY_HANDLERS = new Handler[0];

    private final Map<Class<?>, List<Handler>> handlers = new ConcurrentHashMap<>();
    private final Map<Class<?>, Handler[]> dispatchCache = new ConcurrentHashMap<>();
    private final Map<Object, List<Handler>> owners = Collections.synchronizedMap(new IdentityHashMap<>());
    private final ClassValue<List<HandlerFactory>> subscriberFactories = new ClassValue<List<HandlerFactory>>() {
        @Override
        protected List<HandlerFactory> computeValue(Class<?> type) {
            return findSubscriberFactories(type);
        }
    };
    private final ClassValue<Class<?>[]> dispatchTypes = new ClassValue<Class<?>[]>() {
        @Override
        protected Class<?>[] computeValue(Class<?> type) {
            return findDispatchTypes(type);
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
            dispatchCache.clear();
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

        Handler handler = new Handler(eventType, event -> listener.call(eventType.cast(event)), priority);
        addHandler(handler);
        dispatchCache.clear();
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
            dispatchCache.clear();
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

        Handler[] snapshot = dispatchCache.computeIfAbsent(event.getClass(), this::createDispatchSnapshot);
        for (Handler handler : snapshot) {
            handler.call(event);
        }
        return event;
    }

    private List<Handler> scan(Object owner) {
        List<Handler> found = new ArrayList<>();
        for (HandlerFactory factory : subscriberFactories.get(owner.getClass())) {
            found.add(factory.create(owner));
        }
        return found;
    }

    private List<HandlerFactory> findSubscriberFactories(Class<?> ownerType) {
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

    private HandlerFactory createMethodFactory(Method method, int priority) {
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("@Subscribe method must have exactly one parameter: " + method);
        }

        Class<?> eventType = method.getParameterTypes()[0];
        if (!Event.class.isAssignableFrom(eventType)) {
            throw new IllegalArgumentException("@Subscribe method parameter must implement event: " + method);
        }

        method.setAccessible(true);
        try {
            MethodHandle handle = MethodHandles.lookup().unreflect(method);
            return owner -> new Handler(
                    eventType.asSubclass(Event.class),
                    createMethodInvoker(owner, method, handle),
                    priority
            );
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("unable to access listener method: " + method, exception);
        }
    }

    private HandlerFactory createFieldFactory(Field field, int priority) {
        if (!Listener.class.isAssignableFrom(field.getType())) {
            throw new IllegalArgumentException("@Subscribe field must be a listener: " + field);
        }

        Class<? extends Event> eventType = listenerEventType(field.getGenericType(), field);
        field.setAccessible(true);

        return owner -> createFieldHandler(owner, field, eventType, priority);
    }

    private Handler createFieldHandler(Object owner, Field field, Class<? extends Event> eventType, int priority) {
        try {
            Listener<? extends Event> listener = (Listener<? extends Event>) field.get(owner);
            if (listener == null) {
                throw new IllegalArgumentException("@Subscribe listener field is null: " + field);
            }
            return createListenerHandler(eventType, listener, owner, priority);
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("unable to read listener field: " + field, exception);
        }
    }

    private <T extends Event> Handler createListenerHandler(
            Class<T> eventType,
            Listener<? extends Event> listener,
            Object owner,
            int priority
    ) {
        @SuppressWarnings("unchecked")
        Listener<? super T> typedListener = (Listener<? super T>) listener;
        return new Handler(eventType, event -> typedListener.call(eventType.cast(event)), priority);
    }

    private Class<? extends Event> listenerEventType(Type type, Field field) {
        if (!(type instanceof ParameterizedType)) {
            throw new IllegalArgumentException("@Subscribe listener field must declare its event type: " + field);
        }

        Type eventType = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (!(eventType instanceof Class<?>) || !Event.class.isAssignableFrom((Class<?>) eventType)) {
            throw new IllegalArgumentException("@Subscribe listener field type must implement event: " + field);
        }

        return ((Class<?>) eventType).asSubclass(Event.class);
    }

    private EventInvoker createMethodInvoker(Object owner, Method method, MethodHandle handle) {
        MethodHandle boundHandle = handle
                    .bindTo(owner)
                    .asType(MethodType.methodType(void.class, Event.class));
        return event -> invoke(boundHandle, method, event);
    }

    private void invoke(MethodHandle handle, Method method, Event event) {
        try {
            handle.invokeExact(event);
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Error error) {
            throw error;
        } catch (Throwable throwable) {
            throw new EventDispatchException("listener failed: " + method, throwable);
        }
    }

    private void addHandler(Handler handler) {
        List<Handler> bucket = handlers.computeIfAbsent(handler.eventType, ignored -> new ArrayList<>());
        synchronized (bucket) {
            bucket.add(handler);
            bucket.sort(Handler.ORDER);
        }
    }

    private void removeHandler(Handler handler) {
        List<Handler> bucket = handlers.get(handler.eventType);
        if (bucket == null) {
            return;
        }
        synchronized (bucket) {
            bucket.remove(handler);
            if (bucket.isEmpty()) {
                handlers.remove(handler.eventType, bucket);
            }
        }
    }

    private Handler[] createDispatchSnapshot(Class<?> eventType) {
        List<Handler> matching = new ArrayList<>();
        for (Class<?> dispatchType : dispatchTypes.get(eventType)) {
            List<Handler> bucket = handlers.get(dispatchType);
            if (bucket != null) {
                synchronized (bucket) {
                    matching.addAll(bucket);
                }
            }
        }
        if (matching.isEmpty()) {
            return EMPTY_HANDLERS;
        }
        matching.sort(Handler.ORDER);
        return matching.toArray(new Handler[0]);
    }

    private Class<?>[] findDispatchTypes(Class<?> eventType) {
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

    private void collectEventInterfaces(Class<?> type, Set<Class<?>> types) {
        for (Class<?> interfaceType : type.getInterfaces()) {
            if (Event.class.isAssignableFrom(interfaceType)) {
                types.add(interfaceType);
            }
            collectEventInterfaces(interfaceType, types);
        }
    }

    private void unsubscribe(Handler handler) {
        removeHandler(handler);
        dispatchCache.clear();
    }

    @FunctionalInterface
    private interface EventInvoker {
        void call(Event event);
    }

    @FunctionalInterface
    private interface HandlerFactory {
        Handler create(Object owner);
    }

    private static final class Handler {
        private static final Comparator<Handler> ORDER = Comparator
                .comparingInt((Handler handler) -> handler.priority)
                .thenComparingLong(handler -> handler.sequence);

        private static long nextSequence;

        private final Class<? extends Event> eventType;
        private final EventInvoker invoker;
        private final int priority;
        private final long sequence;

        private Handler(Class<? extends Event> eventType, EventInvoker invoker, int priority) {
            this.eventType = eventType;
            this.invoker = invoker;
            this.priority = priority;
            synchronized (Handler.class) {
                this.sequence = nextSequence++;
            }
        }

        private void call(Event event) {
            invoker.call(event);
        }
    }
}
