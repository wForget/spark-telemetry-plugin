package com.example.spark.telemetry.runtime;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;

/** Tail-admission gate: normal task spans are filtered before the only bounded export queue. */
final class TaskFilteringSpanProcessor implements SpanProcessor {
    static final AttributeKey<Boolean> RETAIN_TASK = AttributeKey.booleanKey("spark.telemetry.task.retained");
    private final SpanProcessor delegate;

    TaskFilteringSpanProcessor(SpanProcessor delegate) {
        this.delegate = delegate;
    }

    @Override public void onStart(Context parentContext, ReadWriteSpan span) {
        if (delegate.isStartRequired()) delegate.onStart(parentContext, span);
    }
    @Override public boolean isStartRequired() { return delegate.isStartRequired(); }

    @Override public void onEnd(ReadableSpan span) {
        Boolean retained = span.getAttribute(RETAIN_TASK);
        if (retained == null || retained.booleanValue()) {
            delegate.onEnd(span);
        }
    }
    @Override public boolean isEndRequired() { return true; }
    @Override public CompletableResultCode forceFlush() { return delegate.forceFlush(); }
    @Override public CompletableResultCode shutdown() { return delegate.shutdown(); }
}
