package cn.wangz.spark.telemetry.signal.traces;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.EventData;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TracePipelineTest {
    @Test
    void disabledPipelineIsANoOp() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), false);

        traces.applicationStarted(1L);
        traces.jobStarted(1, new int[] {2}, 2L);
        traces.stageStarted(2, 0, 3L);

        assertNull(traces.taskStarted(3L, 2, 0, 0, 0, 4L));
        assertEquals(0, processor.started());
        assertDoesNotThrow(() -> traces.close(5L));
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void closeRejectsNewSpansAndLateTaskEndAbandonsHandle() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);

        traces.applicationStarted(1L);
        traces.applicationStarted(1L);
        traces.jobStarted(1, new int[] {2}, 2L);
        traces.jobStarted(1, new int[] {2}, 2L);
        traces.stageStarted(2, 0, 3L);
        traces.stageStarted(2, 0, 3L);
        TaskSpanHandle task = traces.taskStarted(3L, 2, 0, 0, 0, 4L);

        assertNotNull(task);
        assertEquals(4, processor.started());
        traces.close(5L);
        traces.close(6L);
        assertEquals(3, processor.ended());

        traces.applicationStarted(7L);
        traces.jobStarted(2, new int[] {3}, 7L);
        traces.stageStarted(3, 0, 7L);
        assertNull(traces.taskStarted(4L, 3, 0, 0, 0, 7L));
        assertEquals(4, processor.started());

        traces.taskEnded(task, 8L, "success", null, true, false);
        assertEquals(4, processor.ended());
        assertFalse(Span.current().getSpanContext().isValid());
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void telemetryFailuresDoNotEscape() {
        TracePipeline runtimeFailure = new TracePipeline(new FailingTracer(false), true);
        TracePipeline linkageFailure = new TracePipeline(new FailingTracer(true), true);

        assertDoesNotThrow(() -> runtimeFailure.applicationStarted(1L));
        assertDoesNotThrow(() -> runtimeFailure.jobStarted(1, new int[] {2}, 1L));
        assertNull(runtimeFailure.taskStarted(1L, 1, 0, 0, 0, 1L));
        assertDoesNotThrow(() -> linkageFailure.stageStarted(1, 0, 1L));
        assertNull(linkageFailure.taskStarted(1L, 1, 0, 0, 0, 1L));
    }

    @Test
    void stageSpanRecordsAccumulatedTaskMetrics() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);
        StageTaskMetrics metrics = new StageTaskMetrics(
                101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L, 109L);

        traces.stageStarted(2, 1, 3L);
        traces.stageEnded(2, 1, 13L, "success", "", metrics);

        ReadableSpan ended = processor.lastEnded();
        assertEquals("spark.stage", ended.getName());
        assertEquals(Long.valueOf(101L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.executor_run_time_ms")));
        assertEquals(Long.valueOf(102L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.memory_bytes_spilled")));
        assertEquals(Long.valueOf(103L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.disk_bytes_spilled")));
        assertEquals(Long.valueOf(104L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.input.bytes_read")));
        assertEquals(Long.valueOf(105L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.output.bytes_written")));
        assertEquals(Long.valueOf(106L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.read.total_bytes_read")));
        assertEquals(Long.valueOf(107L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.read.fetch_wait_time_ms")));
        assertEquals(Long.valueOf(108L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.write.bytes_written")));
        assertEquals(Long.valueOf(109L), ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.write.write_time_ns")));
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void stageSpanOmitsTaskMetricsWhenUnavailable() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);

        traces.stageStarted(2, 1, 3L);
        traces.stageEnded(2, 1, 13L, "success", "", null);

        ReadableSpan ended = processor.lastEnded();
        assertEquals("spark.stage", ended.getName());
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.executor_run_time_ms")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.memory_bytes_spilled")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.disk_bytes_spilled")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.input.bytes_read")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.output.bytes_written")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.read.total_bytes_read")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.read.fetch_wait_time_ms")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.write.bytes_written")));
        assertNull(ended.getAttribute(AttributeKey.longKey(
                "spark.stage.task_metrics.shuffle.write.write_time_ns")));
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void taskSpanRecordsSlowClassification() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);

        TaskSpanHandle task = traces.taskStarted(3L, 2, 0, 7, 0, 4L);
        traces.taskEnded(task, 8L, "success", null, true, true);

        assertEquals(
                Boolean.TRUE,
                processor.lastEnded().getAttribute(
                        AttributeKey.booleanKey("spark.telemetry.task.slow")));
        assertEquals(StatusCode.UNSET,
                processor.lastEnded().toSpanData().getStatus().getStatusCode());
        assertTrue(processor.lastEnded().toSpanData().getEvents().isEmpty());
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void taskSpanRecordsThrowableAsStandardExceptionEvent() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);
        IllegalStateException exception = new IllegalStateException("intentional failure");

        TaskSpanHandle task = traces.taskStarted(3L, 2, 0, 7, 0, 4L);
        traces.taskEnded(task, 8L, "failure", new TaskFailure(
                "ExceptionFailure",
                "org.apache.spark.ExceptionFailure",
                exception.toString(),
                true,
                exception,
                exception.getClass().getName(),
                exception.getMessage(),
                "ignored fallback stack"), true, false);

        ReadableSpan ended = processor.lastEnded();
        assertEquals(StatusCode.ERROR, ended.toSpanData().getStatus().getStatusCode());
        assertEquals(
                "intentional failure",
                ended.toSpanData().getStatus().getDescription());
        assertEquals(
                "java.lang.IllegalStateException",
                ended.getAttribute(AttributeKey.stringKey("error.type")));
        assertEquals(
                "ExceptionFailure",
                ended.getAttribute(AttributeKey.stringKey("spark.task.failure.type")));
        assertEquals(
                Boolean.TRUE,
                ended.getAttribute(AttributeKey.booleanKey(
                        "spark.task.failure.counts-towards-limit")));
        assertEquals(1, ended.toSpanData().getEvents().size());
        EventData event = ended.toSpanData().getEvents().get(0);
        assertEquals("exception", event.getName());
        assertEquals(
                exception.getClass().getName(),
                event.getAttributes().get(AttributeKey.stringKey("exception.type")));
        assertEquals(
                exception.getMessage(),
                event.getAttributes().get(AttributeKey.stringKey("exception.message")));
        assertTrue(event.getAttributes()
                .get(AttributeKey.stringKey("exception.stacktrace"))
                .contains("TracePipelineTest"));
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void taskSpanFallsBackToPreservedExceptionFieldsWithoutThrowable() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);
        StringBuilder oversizedStack = new StringBuilder();
        for (int index = 0; index < 70_000; index++) oversizedStack.append('s');

        TaskSpanHandle task = traces.taskStarted(3L, 2, 0, 7, 0, 4L);
        traces.taskEnded(task, 8L, "failure", new TaskFailure(
                "ExceptionFailure",
                "org.apache.spark.ExceptionFailure",
                "full error string must not become the status description",
                true,
                null,
                "example.UnserializableException",
                "short message",
                oversizedStack.toString()), true, false);

        ReadableSpan ended = processor.lastEnded();
        assertEquals(
                "short message",
                ended.toSpanData().getStatus().getDescription());
        assertEquals(1, ended.toSpanData().getEvents().size());
        EventData event = ended.toSpanData().getEvents().get(0);
        assertEquals("exception", event.getName());
        assertEquals(
                "example.UnserializableException",
                event.getAttributes().get(AttributeKey.stringKey("exception.type")));
        assertEquals(
                TaskFailure.MAX_STACK_TRACE_LENGTH,
                event.getAttributes()
                        .get(AttributeKey.stringKey("exception.stacktrace"))
                        .length());
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    @Test
    void nonExceptionTaskFailureUsesSparkFailureEvent() {
        CountingSpanProcessor processor = new CountingSpanProcessor();
        SdkTracerProvider provider = provider(processor);
        TracePipeline traces = new TracePipeline(provider.get("test"), true);

        TaskSpanHandle task = traces.taskStarted(3L, 2, 0, 7, 0, 4L);
        traces.taskEnded(task, 8L, "failure", new TaskFailure(
                "FetchFailed", "org.apache.spark.FetchFailed",
                "shuffle block unavailable", true,
                null, "", "", ""), true, false);

        ReadableSpan ended = processor.lastEnded();
        assertEquals(StatusCode.ERROR, ended.toSpanData().getStatus().getStatusCode());
        assertEquals("shuffle block unavailable",
                ended.toSpanData().getStatus().getDescription());
        assertEquals("org.apache.spark.FetchFailed",
                ended.getAttribute(AttributeKey.stringKey("error.type")));
        assertEquals(1, ended.toSpanData().getEvents().size());
        assertEquals("spark.task.failure", ended.toSpanData().getEvents().get(0).getName());
        provider.shutdown().join(1, TimeUnit.SECONDS);
    }

    private static SdkTracerProvider provider(SpanProcessor processor) {
        return SdkTracerProvider.builder().addSpanProcessor(processor).build();
    }

    private static final class CountingSpanProcessor implements SpanProcessor {
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger ended = new AtomicInteger();
        private final AtomicReference<ReadableSpan> lastEnded = new AtomicReference<ReadableSpan>();

        @Override public void onStart(Context parentContext, ReadWriteSpan span) {
            started.incrementAndGet();
        }

        @Override public boolean isStartRequired() { return true; }

        @Override public void onEnd(ReadableSpan span) {
            lastEnded.set(span);
            ended.incrementAndGet();
        }

        @Override public boolean isEndRequired() { return true; }

        @Override public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override public CompletableResultCode forceFlush() {
            return CompletableResultCode.ofSuccess();
        }

        private int started() { return started.get(); }
        private int ended() { return ended.get(); }
        private ReadableSpan lastEnded() { return lastEnded.get(); }
    }

    private static final class FailingTracer implements Tracer {
        private final boolean linkageError;

        private FailingTracer(boolean linkageError) {
            this.linkageError = linkageError;
        }

        @Override public boolean isEnabled() { return true; }

        @Override public SpanBuilder spanBuilder(String spanName) {
            if (linkageError) throw new NoClassDefFoundError("test");
            throw new IllegalStateException("test");
        }
    }
}
