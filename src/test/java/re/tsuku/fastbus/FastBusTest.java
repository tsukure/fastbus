package re.tsuku.fastbus;

import org.junit.Test;
import org.afterlike.openutils.module.handler.ExternalSubscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class FastBusTest {
    @Test
    public void postReturnsPostedEvent() {
        FastBus bus = new FastBus();
        SimpleEvent event = new SimpleEvent();

        assertSame(event, bus.post(event));
    }

    @Test
    public void directListenersReceiveMatchingEvents() {
        FastBus bus = new FastBus();
        Counter counter = new Counter();

        bus.subscribe(SimpleEvent.class, event -> counter.value++);
        bus.post(new SimpleEvent());

        assertEquals(1, counter.value);
    }

    @Test
    public void annotatedMethodsReceiveMatchingEvents() {
        FastBus bus = new FastBus();
        AnnotatedSubscriber subscriber = new AnnotatedSubscriber();

        bus.subscribe(subscriber);
        bus.post(new SimpleEvent());

        assertEquals(1, subscriber.count);
    }

    @Test
    public void privateAnnotatedMethodsReceiveMatchingEvents() {
        FastBus bus = new FastBus();
        PrivateAnnotatedSubscriber subscriber = new PrivateAnnotatedSubscriber();

        bus.subscribe(subscriber);
        bus.post(new SimpleEvent());

        assertEquals(1, subscriber.count);
    }

    @Test
    public void privateAnnotatedMethodsInOtherPackagesReceiveMatchingEvents() {
        FastBus bus = new FastBus();
        ExternalSubscriber subscriber = new ExternalSubscriber();

        bus.subscribe(subscriber);
        bus.post(new SimpleEvent());

        assertEquals(1, subscriber.count());
    }

    @Test
    public void annotatedListenerFieldsReceiveMatchingEvents() {
        FastBus bus = new FastBus();
        FieldSubscriber subscriber = new FieldSubscriber();

        bus.subscribe(subscriber);
        bus.post(new SimpleEvent());

        assertEquals(1, subscriber.count);
    }

    @Test
    public void prioritiesRunLowestNumberFirstThenSubscriptionOrder() {
        FastBus bus = new FastBus();
        List<String> calls = new ArrayList<>();

        bus.subscribe(SimpleEvent.class, event -> calls.add("low"), EventPriority.LOW);
        bus.subscribe(SimpleEvent.class, event -> calls.add("high"), EventPriority.HIGH);
        bus.subscribe(SimpleEvent.class, event -> calls.add("normal-1"));
        bus.subscribe(SimpleEvent.class, event -> calls.add("normal-2"));

        bus.post(new SimpleEvent());

        assertEquals(Arrays.asList("high", "normal-1", "normal-2", "low"), calls);
    }

    @Test
    public void directSubscriptionCanUnsubscribe() {
        FastBus bus = new FastBus();
        Counter counter = new Counter();
        Subscription subscription = bus.subscribe(SimpleEvent.class, event -> counter.value++);

        subscription.unsubscribe();
        bus.post(new SimpleEvent());

        assertEquals(0, counter.value);
    }

    @Test
    public void objectSubscriptionCanUnsubscribe() {
        FastBus bus = new FastBus();
        AnnotatedSubscriber subscriber = new AnnotatedSubscriber();

        bus.subscribe(subscriber);
        assertTrue(bus.subscribed(subscriber));

        bus.unsubscribe(subscriber);
        bus.post(new SimpleEvent());

        assertFalse(bus.subscribed(subscriber));
        assertEquals(0, subscriber.count);
    }

    @Test
    public void duplicateObjectSubscriptionIsIgnored() {
        FastBus bus = new FastBus();
        AnnotatedSubscriber subscriber = new AnnotatedSubscriber();

        bus.subscribe(subscriber);
        bus.subscribe(subscriber);
        bus.post(new SimpleEvent());

        assertEquals(1, subscriber.count);
    }

    @Test
    public void parentEventListenerReceivesChildEvent() {
        FastBus bus = new FastBus();
        Counter counter = new Counter();

        bus.subscribe(ParentEvent.class, event -> counter.value++);
        bus.post(new ChildEvent());

        assertEquals(1, counter.value);
    }

    @Test
    public void eventMarkerListenerReceivesAllEvents() {
        FastBus bus = new FastBus();
        Counter counter = new Counter();

        bus.subscribe(Event.class, event -> counter.value++);
        bus.post(new SimpleEvent());
        bus.post(new ChildEvent());

        assertEquals(2, counter.value);
    }

    @Test
    public void cancellableEventStoresCancellationState() {
        CancelEvent event = new CancelEvent();

        event.setCancelled(true);

        assertTrue(event.isCancelled());
    }

    @Test
    public void invalidAnnotatedMethodFailsDuringSubscribe() {
        FastBus bus = new FastBus();

        try {
            bus.subscribe(new InvalidMethodSubscriber());
            fail("expected invalid subscriber to fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("exactly one parameter"));
        }
    }

    @Test
    public void listenerExceptionsArePropagated() {
        FastBus bus = new FastBus();
        RuntimeException failure = new RuntimeException("boom");

        bus.subscribe(SimpleEvent.class, event -> {
            throw failure;
        });

        try {
            bus.post(new SimpleEvent());
            fail("expected listener failure");
        } catch (RuntimeException expected) {
            assertSame(failure, expected);
        }
    }

    @Test
    public void annotatedRuntimeExceptionsArePropagated() {
        FastBus bus = new FastBus();
        ThrowingSubscriber subscriber = new ThrowingSubscriber();

        bus.subscribe(subscriber);

        try {
            bus.post(new SimpleEvent());
            fail("expected listener failure");
        } catch (IllegalStateException expected) {
            assertEquals("boom", expected.getMessage());
        }
    }

    @Test
    public void annotatedCheckedExceptionsAreWrapped() {
        FastBus bus = new FastBus();
        CheckedThrowingSubscriber subscriber = new CheckedThrowingSubscriber();

        bus.subscribe(subscriber);

        try {
            bus.post(new SimpleEvent());
            fail("expected listener failure");
        } catch (EventDispatchException expected) {
            assertEquals("checked", expected.getCause().getMessage());
        }
    }

    private interface ParentEvent extends Event {
    }

    public static final class SimpleEvent implements Event {
    }

    private static final class ChildEvent implements ParentEvent {
    }

    private static final class CancelEvent extends CancellableEvent {
    }

    private static final class Counter {
        int value;
    }

    private static final class AnnotatedSubscriber {
        int count;

        @Subscribe
        public void onSimple(SimpleEvent event) {
            count++;
        }
    }

    private static final class PrivateAnnotatedSubscriber {
        int count;

        @Subscribe
        private void onSimple(SimpleEvent event) {
            count++;
        }
    }

    private static final class FieldSubscriber {
        int count;

        @Subscribe
        private final Listener<SimpleEvent> listener = event -> count++;
    }

    private static final class InvalidMethodSubscriber {
        @Subscribe
        public void invalid() {
        }
    }

    private static final class ThrowingSubscriber {
        @Subscribe
        public void onSimple(SimpleEvent event) {
            throw new IllegalStateException("boom");
        }
    }

    private static final class CheckedThrowingSubscriber {
        @Subscribe
        public void onSimple(SimpleEvent event) throws Exception {
            throw new Exception("checked");
        }
    }
}
