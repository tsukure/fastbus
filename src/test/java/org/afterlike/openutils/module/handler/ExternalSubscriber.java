package org.afterlike.openutils.module.handler;

import re.tsuku.fastbus.FastBusTest.SimpleEvent;
import re.tsuku.fastbus.Subscribe;

public final class ExternalSubscriber {
    private int count;

    public int count() {
        return count;
    }

    @Subscribe
    private void onSimple(SimpleEvent event) {
        count++;
    }
}
