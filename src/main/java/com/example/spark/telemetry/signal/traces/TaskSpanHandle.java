package com.example.spark.telemetry.signal.traces;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Opaque Java handle kept in Executor ThreadLocal; no OTel type crosses into Spark events. */
public final class TaskSpanHandle {
    private final Span span;
    private final Scope scope;
    private final AtomicBoolean ended = new AtomicBoolean();

    TaskSpanHandle(Span span, Scope scope) {
        this.span = span;
        this.scope = scope;
    }

    public String traceId() { return span.getSpanContext().getTraceId(); }
    public String spanId() { return span.getSpanContext().getSpanId(); }

    public void abandon(long endEpochNanos) {
        end(endEpochNanos, "cancelled", "telemetry runtime stopped", false);
    }

    void end(long endEpochNanos, String outcome, String failure, boolean retain) {
        if (!ended.compareAndSet(false, true)) return;
        try {
            scope.close();
        } finally {
            span.setAttribute("outcome", outcome == null ? "unknown" : outcome);
            span.setAttribute(TaskFilteringSpanProcessor.RETAIN_TASK, retain);
            if (!"success".equals(outcome)) {
                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, failure == null ? "" : failure);
            }
            span.end(endEpochNanos, TimeUnit.NANOSECONDS);
        }
    }
}
