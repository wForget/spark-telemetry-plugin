package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class PluginSelfMetricsTest {

    @Test
    void recordsCounters() {
        PluginSelfMetrics metrics = new PluginSelfMetrics();
        metrics.recordReceived();
        metrics.recordReceived();
        metrics.recordDropped();
        metrics.recordExported(2);
        metrics.recordFailure();

        PluginSelfMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(2L, snapshot.eventsReceived());
        assertEquals(2L, snapshot.eventsExported());
        assertEquals(1L, snapshot.eventsDropped());
        assertEquals(1L, snapshot.exportFailures());
    }

    @Test
    void counterUpdatesAreThreadSafe() {
        PluginSelfMetrics metrics = new PluginSelfMetrics();

        IntStream.range(0, 10_000).parallel().forEach(ignored -> metrics.recordReceived());

        assertEquals(10_000L, metrics.snapshot().eventsReceived());
    }
}
