package com.example.spark.telemetry.runtime;

import com.example.spark.telemetry.signal.traces.TaskSpanHandle;
import com.example.spark.telemetry.signal.traces.TraceSink;
import org.apache.spark.telemetry.config.TelemetryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeferredTelemetrySinkTest {
    @Test
    void drainsBootstrapEventsInOrderAndCopiesMutableInputs() {
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        int[] stages = new int[] {2};
        sink.jobStarted(1, stages, 10L);
        stages[0] = 99;
        sink.stageStarted(2, 0, 11L);
        sink.jobEnded(1, 20L, "success", "");

        RecordingTraceSink traces = new RecordingTraceSink();
        sink.bind(traces);
        sink.stageEnded(2, 0, 21L, "success", "");

        assertEquals(Arrays.asList(
                "job-start:1:[2]",
                "stage-start:2:0",
                "job-end:1:success",
                "stage-end:2:0:success"), traces.events());

        sink.close();
        sink.applicationEnded(30L);
        assertEquals(4, traces.events().size());
        assertDoesNotThrow(sink::close);
    }

    @Test
    void preservesOrderForEventsSubmittedWhileBinding() throws Exception {
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.jobStarted(1, new int[] {2}, 10L);
        CountDownLatch applyingBootstrap = new CountDownLatch(1);
        CountDownLatch continueBinding = new CountDownLatch(1);
        RecordingTraceSink traces = new RecordingTraceSink(applyingBootstrap, continueBinding);

        Thread binder = new Thread(new Runnable() {
            @Override public void run() { sink.bind(traces); }
        }, "deferred-trace-bind-test");
        binder.start();
        assertTrue(applyingBootstrap.await(5, TimeUnit.SECONDS));
        sink.jobEnded(1, 20L, "success", "");
        continueBinding.countDown();
        binder.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(binder.isAlive());
        assertEquals(Arrays.asList(
                "job-start:1:[2]",
                "job-end:1:success"), traces.events());
    }

    @Test
    void dropsNewestBootstrapEventWhenCapacityIsFull() {
        DeferredTelemetrySink sink = new DeferredTelemetrySink(2);
        sink.stageStarted(1, 0, 10L);
        sink.stageStarted(2, 0, 11L);
        sink.stageStarted(3, 0, 12L);
        RecordingTraceSink traces = new RecordingTraceSink();

        sink.bind(traces);

        assertEquals(Arrays.asList(
                "stage-start:1:0",
                "stage-start:2:0"), traces.events());
    }

    @Test
    void allowsMetricsToBeSuppressedForARegistrySharedWithTheDriver() {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.METRICS_ENABLED().key(), "true");
        values.put(TelemetryConfig.LOGS_ENABLED().key(), "false");
        values.put(TelemetryConfig.TRACES_ENABLED().key(), "false");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("test", "app-1");

        TelemetryRuntime runtime = TelemetryRuntime.create(
                config, ResourceIdentity.executor(config, "app-1", "driver"), null);

        assertNull(runtime.traces().taskStarted(1L, 1, 0, 0, 0, 1L));
        assertDoesNotThrow(() -> runtime.close(Duration.ofMillis(10)));
    }

    private static final class RecordingTraceSink implements TraceSink {
        private final List<String> events = Collections.synchronizedList(new ArrayList<String>());
        private final CountDownLatch applyingBootstrap;
        private final CountDownLatch continueBinding;

        private RecordingTraceSink() {
            this(null, null);
        }

        private RecordingTraceSink(
                CountDownLatch applyingBootstrap, CountDownLatch continueBinding) {
            this.applyingBootstrap = applyingBootstrap;
            this.continueBinding = continueBinding;
        }

        @Override public void applicationStarted(long epochMillis) {
            events.add("application-start");
        }

        @Override public void applicationEnded(long epochMillis) {
            events.add("application-end");
        }

        @Override public void jobStarted(int jobId, int[] stageIds, long epochMillis) {
            if (applyingBootstrap != null) {
                applyingBootstrap.countDown();
                try {
                    continueBinding.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            events.add("job-start:" + jobId + ":" + Arrays.toString(stageIds));
        }

        @Override public void jobEnded(
                int jobId, long epochMillis, String outcome, String failure) {
            events.add("job-end:" + jobId + ":" + outcome);
        }

        @Override public void stageStarted(int stageId, int attempt, long epochMillis) {
            events.add("stage-start:" + stageId + ":" + attempt);
        }

        @Override public void stageEnded(
                int stageId, int attempt, long epochMillis, String outcome, String failure) {
            events.add("stage-end:" + stageId + ":" + attempt + ":" + outcome);
        }

        @Override public TaskSpanHandle taskStarted(
                long taskAttemptId,
                int stageId,
                int stageAttempt,
                int partitionId,
                int attemptNumber,
                long startEpochNanos) {
            return null;
        }

        @Override public void taskEnded(
                TaskSpanHandle handle,
                long endEpochNanos,
                String outcome,
                String failure,
                boolean retain) {
        }

        private List<String> events() {
            synchronized (events) {
                return new ArrayList<String>(events);
            }
        }
    }
}
