package re.tsuku.fastbus;

public final class EventDispatchException extends RuntimeException {
    public EventDispatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
