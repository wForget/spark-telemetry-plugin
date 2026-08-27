package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class PluginSelfMetricsTest {

    @Test
    void recordsCountersAndReadsLiveQueueGauge() {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(3);
        PluginSelfMetrics metrics = new PluginSelfMetrics();
        metrics.setQueueGauge(PluginSelfMetrics.queueGauge(queue));
        queue.offer(1);
        metrics.recordReceived(2);
        metrics.recordDropped();
        metrics.recordRetry();
        metrics.recordSuccessfulBatch(2, 50);

        PluginSelfMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(2L, snapshot.eventsReceived());
        assertEquals(2L, snapshot.eventsExported());
        assertEquals(1L, snapshot.eventsDropped());
        assertEquals(1L, snapshot.exportRetries());
        assertEquals(1L, snapshot.batchesExported());
        assertEquals(50L, snapshot.exportDurationNanos());
        assertEquals(2L, snapshot.lastBatchSize());
        assertEquals(1, snapshot.queueSize());
        assertEquals(3, snapshot.queueCapacity());
    }

    @Test
    void counterUpdatesAreThreadSafe() {
        PluginSelfMetrics metrics = new PluginSelfMetrics();

        IntStream.range(0, 10_000).parallel().forEach(ignored -> metrics.recordReceived());

        assertEquals(10_000L, metrics.snapshot().eventsReceived());
    }
}
