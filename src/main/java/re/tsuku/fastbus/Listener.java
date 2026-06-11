package re.tsuku.fastbus;

@FunctionalInterface
public interface Listener<T extends Event> {
    void call(T event);
}
