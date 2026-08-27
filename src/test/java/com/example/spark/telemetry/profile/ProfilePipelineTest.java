package com.example.spark.telemetry.profile;

import com.example.spark.telemetry.reliability.PluginSelfMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ProfilePipelineTest {
    @Test
    void overloadSkipsWholeWindowsAndShutdownIsBounded() throws Exception {
        CountDownLatch exportStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ProfileExporter exporter = new ProfileExporter() {
            @Override public ExportResult export(ProfileBatch batch) {
                exportStarted.countDown();
                try { release.await(); } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return ExportResult.failure(true, -1, "interrupted");
                }
                return ExportResult.success(200);
            }
            @Override public void close() { release.countDown(); }
        };
        PluginSelfMetrics metrics = new PluginSelfMetrics();
        ProfilePipeline pipeline = new ProfilePipeline(exporter, 1, Duration.ofMillis(1), metrics);
        ProfileBatch batch = batch();
        assertTrue(pipeline.offer(batch));
        assertTrue(exportStarted.await(1, TimeUnit.SECONDS));
        assertTrue(pipeline.offer(batch));
        assertFalse(pipeline.offer(batch));

        long start = System.nanoTime();
        pipeline.close(Duration.ofMillis(20));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsedMillis < 250L, "elapsed=" + elapsedMillis);
        assertTrue(metrics.snapshot().eventsDropped() >= 1L);
        release.countDown();
    }

    @Test
    void batchDefensivelyCopiesPayload() {
        byte[] payload = {1, 2, 3};
        ProfileBatch batch = new ProfileBatch(
                "spark", "pprof", "application/octet-stream", 1L, 2L, 19,
                Collections.<String, String>emptyMap(), payload);
        payload[0] = 9;
        assertArrayEquals(new byte[] {1, 2, 3}, batch.payload());
        byte[] copy = batch.payload();
        copy[1] = 9;
        assertArrayEquals(new byte[] {1, 2, 3}, batch.payload());
    }

    private static ProfileBatch batch() {
        return new ProfileBatch(
                "spark", "pprof", "application/octet-stream", 1L, 2L, 19,
                Collections.<String, String>emptyMap(), new byte[] {1});
    }
}
