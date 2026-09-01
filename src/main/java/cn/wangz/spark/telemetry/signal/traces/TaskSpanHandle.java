package cn.wangz.spark.telemetry.signal.traces;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Opaque Java handle kept in Executor ThreadLocal; no OTel type crosses into Spark events. */
public final class TaskSpanHandle {
    private static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
    private static final AttributeKey<String> FAILURE_TYPE =
            AttributeKey.stringKey("spark.task.failure.type");
    private static final AttributeKey<String> FAILURE_MESSAGE =
            AttributeKey.stringKey("spark.task.failure.message");
    private static final AttributeKey<Boolean> FAILURE_COUNTS_TOWARDS_LIMIT =
            AttributeKey.booleanKey("spark.task.failure.counts-towards-limit");
    private static final AttributeKey<String> EXCEPTION_TYPE =
            AttributeKey.stringKey("exception.type");
    private static final AttributeKey<String> EXCEPTION_MESSAGE =
            AttributeKey.stringKey("exception.message");
    private static final AttributeKey<String> EXCEPTION_STACKTRACE =
            AttributeKey.stringKey("exception.stacktrace");

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
        end(endEpochNanos, "cancelled", null, false, false);
    }

    void end(
            long endEpochNanos,
            String outcome,
            TaskFailure failure,
            boolean retain,
            boolean slow) {
        if (!ended.compareAndSet(false, true)) return;
        try {
            scope.close();
        } finally {
            try {
                span.setAttribute("outcome", outcome == null ? "unknown" : outcome);
                span.setAttribute(TaskFilteringSpanProcessor.RETAIN_TASK, retain);
                span.setAttribute("spark.telemetry.task.slow", slow);
                if (!"success".equals(outcome)) {
                    span.setStatus(StatusCode.ERROR,
                            failure == null ? "" : failure.statusDescription());
                    try {
                        recordFailure(failure);
                    } catch (RuntimeException | LinkageError ignored) {
                        // A hostile Throwable or telemetry ABI issue must not alter task execution.
                    }
                }
            } finally {
                span.end(endEpochNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    private void recordFailure(TaskFailure failure) {
        if (failure == null) return;
        put(FAILURE_TYPE, failure.reasonType());
        put(FAILURE_MESSAGE, failure.message());
        span.setAttribute(
                FAILURE_COUNTS_TOWARDS_LIMIT, failure.countsTowardsTaskFailures());
        put(ERROR_TYPE, failure.errorType());

        if (failure.exception() != null) {
            AttributesBuilder attributes = Attributes.builder();
            put(attributes, EXCEPTION_TYPE, failure.exceptionType());
            span.recordException(failure.exception(), attributes.build());
        } else if (failure.hasExceptionDetails()) {
            AttributesBuilder attributes = Attributes.builder();
            put(attributes, EXCEPTION_TYPE, failure.exceptionType());
            put(attributes, EXCEPTION_MESSAGE, failure.exceptionMessage());
            put(attributes, EXCEPTION_STACKTRACE, failure.exceptionStackTrace());
            span.addEvent("exception", attributes.build());
        } else {
            AttributesBuilder attributes = Attributes.builder()
                    .put(FAILURE_COUNTS_TOWARDS_LIMIT, failure.countsTowardsTaskFailures());
            put(attributes, FAILURE_TYPE, failure.reasonType());
            put(attributes, FAILURE_MESSAGE, failure.message());
            span.addEvent("spark.task.failure", attributes.build());
        }
    }

    private void put(AttributeKey<String> key, String value) {
        if (value != null && !value.isEmpty()) span.setAttribute(key, value);
    }

    private static void put(
            AttributesBuilder attributes, AttributeKey<String> key, String value) {
        if (value != null && !value.isEmpty()) attributes.put(key, value);
    }
}
