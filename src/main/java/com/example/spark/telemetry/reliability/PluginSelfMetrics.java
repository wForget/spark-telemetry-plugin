package com.example.spark.telemetry.reliability;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe, dependency-free counters for one telemetry signal pipeline. */
public final class PluginSelfMetrics {

    public static final class Snapshot {
        private final long eventsReceived;
        private final long eventsExported;
        private final long eventsDropped;
        private final long exportFailures;

        Snapshot(
                long eventsReceived,
                long eventsExported,
                long eventsDropped,
                long exportFailures) {
            this.eventsReceived = eventsReceived;
            this.eventsExported = eventsExported;
            this.eventsDropped = eventsDropped;
            this.exportFailures = exportFailures;
        }

        public long eventsReceived() { return eventsReceived; }

        public long eventsExported() { return eventsExported; }

        public long eventsDropped() { return eventsDropped; }

        public long exportFailures() { return exportFailures; }
    }

    private final LongAdder eventsReceived = new LongAdder();
    private final LongAdder eventsExported = new LongAdder();
    private final LongAdder eventsDropped = new LongAdder();
    private final LongAdder exportFailures = new LongAdder();

    public void recordReceived() { eventsReceived.increment(); }

    public void recordExported(long count) { addPositive(eventsExported, count); }

    public void recordDropped() { eventsDropped.increment(); }

    public void recordDropped(long count) { addPositive(eventsDropped, count); }

    public void recordFailure() { exportFailures.increment(); }

    public Snapshot snapshot() {
        return new Snapshot(
                eventsReceived.sum(),
                eventsExported.sum(),
                eventsDropped.sum(),
                exportFailures.sum());
    }

    private static void addPositive(LongAdder adder, long count) {
        if (count > 0L) {
            adder.add(count);
        }
    }
}
