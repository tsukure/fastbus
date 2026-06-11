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
    public CounterEvent post_direct_listener(DirectPostState state) {
        return state.bus.post(state.event);
    }

    @Benchmark
    public CounterEvent post_annotated_method(AnnotatedPostState state) {
        return state.bus.post(state.event);
    }

    @Benchmark
    public FastBus subscribe_direct_listener(SubscribeState state) {
        FastBus bus = new FastBus();
        Subscription subscription = bus.subscribe(CounterEvent.class, state.directListener);
        subscription.unsubscribe();
        return bus;
    }

    @Benchmark
    public FastBus subscribe_annotated_method(SubscribeState state) {
        FastBus bus = new FastBus();
        bus.subscribe(state.annotatedListener);
        bus.unsubscribe(state.annotatedListener);
        return bus;
    }

    @State(Scope.Thread)
    public static class DirectPostState {
        final FastBus bus = new FastBus();
        final CounterEvent event = new CounterEvent();
        int counter;

        @Setup(Level.Trial)
        public void setup() {
            bus.subscribe(CounterEvent.class, event -> counter++);
        }
    }

    @State(Scope.Thread)
    public static class AnnotatedPostState {
        final FastBus bus = new FastBus();
        final CounterEvent event = new CounterEvent();
        final AnnotatedListener listener = new AnnotatedListener();

        @Setup(Level.Trial)
        public void setup() {
            bus.subscribe(listener);
        }
    }

    @State(Scope.Thread)
    public static class SubscribeState {
        final Listener<CounterEvent> directListener = event -> {
        };
        final AnnotatedListener annotatedListener = new AnnotatedListener();
    }

    public static final class AnnotatedListener {
        int counter;

        @Subscribe
        public void onCounter(CounterEvent event) {
            counter++;
        }
    }

    public static final class CounterEvent implements Event {
    }
}
