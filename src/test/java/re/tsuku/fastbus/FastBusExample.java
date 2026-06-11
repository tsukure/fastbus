package re.tsuku.fastbus;

public final class FastBusExample {
    public static void main(String[] args) {
        FastBus bus = new FastBus();
        ExampleListener listener = new ExampleListener();

        bus.subscribe(listener);
        bus.post(new ExampleEvent("hello"));
        bus.unsubscribe(listener);
    }

    public static final class ExampleEvent implements Event {
        private final String message;

        public ExampleEvent(String message) {
            this.message = message;
        }

        public String message() {
            return message;
        }
    }

    public static final class ExampleListener {
        @Subscribe
        public void onExample(ExampleEvent event) {
            System.out.println(event.message());
        }
    }
}
