package re.tsuku.fastbus;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class FastBusBenchmark {
    @Benchmark
    public CounterEvent post_fastbus_direct_listener(FastBusDirectPostState state) {
        return state.bus.post(state.event);
    }

    @Benchmark
    public CounterEvent post_fastbus_annotated_method(FastBusAnnotatedPostState state) {
        return state.bus.post(state.event);
    }

    @Benchmark
    public CounterEvent post_guava_eventbus(GuavaPostState state) {
        state.bus.post(state.event);
        return state.event;
    }

    @Benchmark
    public CounterEvent post_greenrobot_eventbus(GreenrobotPostState state) {
        state.bus.post(state.event);
        return state.event;
    }

    @Benchmark
    public FastBus subscribe_fastbus_direct_listener(SubscribeState state) {
        FastBus bus = new FastBus();
        Subscription subscription = bus.subscribe(CounterEvent.class, state.directListener);
        subscription.unsubscribe();
        return bus;
    }

    @Benchmark
    public FastBus subscribe_fastbus_annotated_method(SubscribeState state) {
        FastBus bus = new FastBus();
        bus.subscribe(state.annotatedListener);
        bus.unsubscribe(state.annotatedListener);
        return bus;
    }

    @Benchmark
    public com.google.common.eventbus.EventBus subscribe_guava_eventbus(SubscribeState state) {
        com.google.common.eventbus.EventBus bus = new com.google.common.eventbus.EventBus();
        bus.register(state.guavaListener);
        bus.unregister(state.guavaListener);
        return bus;
    }

    @Benchmark
    public org.greenrobot.eventbus.EventBus subscribe_greenrobot_eventbus(SubscribeState state) {
        org.greenrobot.eventbus.EventBus bus = org.greenrobot.eventbus.EventBus.builder().build();
        bus.register(state.greenrobotListener);
        bus.unregister(state.greenrobotListener);
        return bus;
    }

    @State(Scope.Thread)
    public static class FastBusDirectPostState {
        final FastBus bus = new FastBus();
        final CounterEvent event = new CounterEvent();
        int counter;

        @Setup(Level.Trial)
        public void setup() {
            bus.subscribe(CounterEvent.class, event -> counter++);
        }
    }

    @State(Scope.Thread)
    public static class FastBusAnnotatedPostState {
        final FastBus bus = new FastBus();
        final CounterEvent event = new CounterEvent();
        final AnnotatedListener listener = new AnnotatedListener();

        @Setup(Level.Trial)
        public void setup() {
            bus.subscribe(listener);
        }
    }

    @State(Scope.Thread)
    public static class GuavaPostState {
        final com.google.common.eventbus.EventBus bus = new com.google.common.eventbus.EventBus();
        final CounterEvent event = new CounterEvent();
        final GuavaListener listener = new GuavaListener();

        @Setup(Level.Trial)
        public void setup() {
            bus.register(listener);
        }
    }

    @State(Scope.Thread)
    public static class GreenrobotPostState {
        final org.greenrobot.eventbus.EventBus bus = org.greenrobot.eventbus.EventBus.builder().build();
        final CounterEvent event = new CounterEvent();
        final GreenrobotListener listener = new GreenrobotListener();

        @Setup(Level.Trial)
        public void setup() {
            bus.register(listener);
        }
    }

    @State(Scope.Thread)
    public static class SubscribeState {
        final Listener<CounterEvent> directListener = event -> {
        };
        final AnnotatedListener annotatedListener = new AnnotatedListener();
        final GuavaListener guavaListener = new GuavaListener();
        final GreenrobotListener greenrobotListener = new GreenrobotListener();
    }

    public static final class AnnotatedListener {
        int counter;

        @Subscribe
        public void onCounter(CounterEvent event) {
            counter++;
        }
    }

    public static final class GuavaListener {
        int counter;

        @com.google.common.eventbus.Subscribe
        public void onCounter(CounterEvent event) {
            counter++;
        }
    }

    public static final class GreenrobotListener {
        int counter;

        @org.greenrobot.eventbus.Subscribe
        public void onCounter(CounterEvent event) {
            counter++;
        }
    }

    public static final class CounterEvent implements Event {
    }
}
