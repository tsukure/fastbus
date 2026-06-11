package re.tsuku.fastbus;

/**
 * priority values for listener execution. lower numbers run first.
 */
public final class EventPriority {
    public static final int HIGHEST = 0;
    public static final int HIGH = 100;
    public static final int NORMAL = 200;
    public static final int LOW = 300;
    public static final int LOWEST = 400;

    private EventPriority() {
    }
}
