package com.example.spark.telemetry.signal.traces;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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

        traces.taskEnded(task, 8L, "success", "", true);
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

    private static SdkTracerProvider provider(SpanProcessor processor) {
        return SdkTracerProvider.builder().addSpanProcessor(processor).build();
    }

    private static final class CountingSpanProcessor implements SpanProcessor {
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger ended = new AtomicInteger();

        @Override public void onStart(Context parentContext, ReadWriteSpan span) {
            started.incrementAndGet();
        }

        @Override public boolean isStartRequired() { return true; }

        @Override public void onEnd(ReadableSpan span) { ended.incrementAndGet(); }

        @Override public boolean isEndRequired() { return true; }

        @Override public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override public CompletableResultCode forceFlush() {
            return CompletableResultCode.ofSuccess();
        }

        private int started() { return started.get(); }
        private int ended() { return ended.get(); }
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
