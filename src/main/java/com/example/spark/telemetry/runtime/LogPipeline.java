package com.example.spark.telemetry.runtime;

import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.spark.telemetry.config.TelemetryLogLevel;

/** Converts immutable Log4j snapshots to OpenTelemetry log records. */
public final class LogPipeline {
    private final Logger logger;
    private final TelemetryLogLevel minimumLevel;

    LogPipeline(Logger logger, TelemetryLogLevel minimumLevel) {
        this.logger = logger;
        this.minimumLevel = minimumLevel;
    }

    public void emit(
            long epochMillis,
            TelemetryLogLevel level,
            String loggerName,
            String message,
            String exceptionType,
            String exceptionMessage,
            String exceptionStack,
            Map<String, String> context) {
        if (!minimumLevel.includes(level) || excluded(loggerName)) return;
        LogRecordBuilder record = logger.logRecordBuilder()
                .setTimestamp(epochMillis, TimeUnit.MILLISECONDS)
                .setObservedTimestamp(Instant.now())
                .setSeverity(severity(level))
                .setSeverityText(level.name())
                .setBody(limit(message, 65536))
                .setAttribute("logger.name", safe(loggerName));
        put(record, "exception.type", exceptionType);
        put(record, "exception.message", limit(exceptionMessage, 8192));
        put(record, "exception.stacktrace", limit(exceptionStack, 65536));
        copyAllowedContext(record, context);
        record.emit();
    }

    private static void copyAllowedContext(LogRecordBuilder record, Map<String, String> context) {
        if (context == null) return;
        put(record, "spark.job.id", context.get("spark.job.id"));
        put(record, "spark.stage.id", context.get("spark.stage.id"));
        put(record, "spark.task.attempt.id", context.get("spark.task.attempt.id"));
        put(record, "trace_id", context.get("trace_id"));
        put(record, "span_id", context.get("span_id"));
    }

    private static boolean excluded(String loggerName) {
        return loggerName != null && (loggerName.startsWith("com.example.spark.telemetry")
                || loggerName.startsWith("io.opentelemetry"));
    }

    private static Severity severity(TelemetryLogLevel level) {
        switch (level) {
            case TRACE: return Severity.TRACE;
            case DEBUG: return Severity.DEBUG;
            case WARN: return Severity.WARN;
            case ERROR: return Severity.ERROR;
            case FATAL: return Severity.FATAL;
            default: return Severity.INFO;
        }
    }

    private static void put(LogRecordBuilder record, String key, String value) {
        if (value != null && !value.isEmpty()) record.setAttribute(key, value);
    }
    private static String safe(String value) { return value == null ? "" : value; }
    private static String limit(String value, int max) {
        if (value == null) return "";
        return value.length() <= max ? value : value.substring(0, max);
    }
}
