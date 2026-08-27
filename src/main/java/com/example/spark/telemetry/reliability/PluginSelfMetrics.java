package com.example.spark.telemetry.reliability;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Thread-safe, dependency-free counters for one telemetry signal pipeline. */
public final class PluginSelfMetrics {

    public interface QueueGauge {
        int size();

        int capacity();
    }

    public static final class Snapshot {
        private final long eventsReceived;
        private final long eventsExported;
        private final long eventsDropped;
        private final long exportRetries;
        private final long exportFailures;
        private final long batchesExported;
        private final long exportDurationNanos;
        private final long lastBatchSize;
        private final int queueSize;
        private final int queueCapacity;

        private Snapshot(
                long eventsReceived,
                long eventsExported,
                long eventsDropped,
                long exportRetries,
                long exportFailures,
                long batchesExported,
                long exportDurationNanos,
                long lastBatchSize,
                int queueSize,
                int queueCapacity) {
            this.eventsReceived = eventsReceived;
            this.eventsExported = eventsExported;
            this.eventsDropped = eventsDropped;
            this.exportRetries = exportRetries;
            this.exportFailures = exportFailures;
            this.batchesExported = batchesExported;
            this.exportDurationNanos = exportDurationNanos;
            this.lastBatchSize = lastBatchSize;
            this.queueSize = queueSize;
            this.queueCapacity = queueCapacity;
        }

        public long eventsReceived() { return eventsReceived; }

        public long eventsExported() { return eventsExported; }

        public long eventsDropped() { return eventsDropped; }

        public long exportRetries() { return exportRetries; }

        public long exportFailures() { return exportFailures; }

        public long batchesExported() { return batchesExported; }

        public long exportDurationNanos() { return exportDurationNanos; }

        public long lastBatchSize() { return lastBatchSize; }

        public int queueSize() { return queueSize; }

        public int queueCapacity() { return queueCapacity; }
    }

    private static final QueueGauge EMPTY_QUEUE_GAUGE = new QueueGauge() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public int capacity() {
            return 0;
        }
    };

    private final LongAdder eventsReceived = new LongAdder();
    private final LongAdder eventsExported = new LongAdder();
    private final LongAdder eventsDropped = new LongAdder();
    private final LongAdder exportRetries = new LongAdder();
    private final LongAdder exportFailures = new LongAdder();
    private final LongAdder batchesExported = new LongAdder();
    private final LongAdder exportDurationNanos = new LongAdder();
    private final AtomicLong lastBatchSize = new AtomicLong();
    private volatile QueueGauge queueGauge = EMPTY_QUEUE_GAUGE;

    public void setQueueGauge(QueueGauge queueGauge) {
        this.queueGauge = queueGauge == null ? EMPTY_QUEUE_GAUGE : queueGauge;
    }

    public void recordReceived() { eventsReceived.increment(); }

    public void recordReceived(long count) { addPositive(eventsReceived, count); }

    public void recordExported(long count) { addPositive(eventsExported, count); }

    public void recordDropped() { eventsDropped.increment(); }

    public void recordDropped(long count) { addPositive(eventsDropped, count); }

    public void recordRetry() { exportRetries.increment(); }

    public void recordFailure() { exportFailures.increment(); }

    public void recordSuccessfulBatch(int batchSize, long durationNanos) {
        if (batchSize < 0 || durationNanos < 0L) {
            return;
        }
        batchesExported.increment();
        eventsExported.add(batchSize);
        exportDurationNanos.add(durationNanos);
        lastBatchSize.set(batchSize);
    }

    public Snapshot snapshot() {
        QueueGauge gauge = queueGauge;
        int size = safeGaugeValue(gauge, true);
        int capacity = safeGaugeValue(gauge, false);
        return new Snapshot(
                eventsReceived.sum(),
                eventsExported.sum(),
                eventsDropped.sum(),
                exportRetries.sum(),
                exportFailures.sum(),
                batchesExported.sum(),
                exportDurationNanos.sum(),
                lastBatchSize.get(),
                size,
                capacity);
    }

    public static QueueGauge queueGauge(final BoundedSignalQueue<?> queue) {
        if (queue == null) {
            return EMPTY_QUEUE_GAUGE;
        }
        return new QueueGauge() {
            @Override
            public int size() {
                return queue.size();
            }

            @Override
            public int capacity() {
                return queue.capacity();
            }
        };
    }

    /** Listener adapter for wiring a processor into these metrics. */
    public BatchProcessor.Listener batchListener() {
        return new BatchProcessor.Listener() {
            @Override
            public void onExportSuccess(int batchSize, long durationNanos) {
                recordSuccessfulBatch(batchSize, durationNanos);
            }

            @Override
            public void onExportFailure(int batchSize, Throwable failure, long durationNanos) {
                recordFailure();
                recordDropped(batchSize);
            }

            @Override
            public void onDiscard(int itemCount) {
                recordDropped(itemCount);
            }
        };
    }

    private static void addPositive(LongAdder adder, long count) {
        if (count > 0L) {
            adder.add(count);
        }
    }

    private static int safeGaugeValue(QueueGauge gauge, boolean size) {
        try {
            return Math.max(0, size ? gauge.size() : gauge.capacity());
        } catch (RuntimeException ignored) {
            return 0;
        }
    }
}
