package re.tsuku.fastbus;

/**
 * base event type for events that can be cancelled by listeners.
 */
public abstract class CancellableEvent implements Event {
    private boolean cancelled;

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
