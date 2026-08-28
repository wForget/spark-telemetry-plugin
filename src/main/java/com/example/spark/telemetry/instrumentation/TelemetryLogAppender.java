package com.example.spark.telemetry.instrumentation;

import com.example.spark.telemetry.runtime.LogPipeline;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.telemetry.config.TelemetryLogLevel;

final class TelemetryLogAppender extends AbstractAppender {
    private final LogPipeline pipeline;
    private final ThreadLocal<Boolean> recursive = new ThreadLocal<Boolean>();

    TelemetryLogAppender(String name, LogPipeline pipeline) {
        super(name, null, null, true, Property.EMPTY_ARRAY);
        this.pipeline = pipeline;
    }

    @Override
    public void append(LogEvent event) {
        if (Boolean.TRUE.equals(recursive.get())) return;
        recursive.set(Boolean.TRUE);
        try {
            Throwable thrown = event.getThrown();
            pipeline.emit(
                    event.getTimeMillis(),
                    level(event.getLevel()),
                    event.getLoggerName(),
                    event.getMessage() == null ? "" : event.getMessage().getFormattedMessage(),
                    thrown == null ? "" : thrown.getClass().getName(),
                    thrown == null ? "" : thrown.getMessage(),
                    thrown == null ? "" : stackTrace(thrown),
                    context(event));
        } catch (RuntimeException ignored) {
            // Logging must never fail Spark or recurse through the exporter.
        } catch (LinkageError ignored) {
            // A Log4j/OTel ABI mismatch must not break the application's logging call.
        } finally {
            recursive.remove();
        }
    }

    private static Map<String, String> context(LogEvent event) {
        return event.getContextData() == null
                ? Collections.<String, String>emptyMap()
                : event.getContextData().toMap();
    }

    private static TelemetryLogLevel level(Level level) {
        if (level == null) return TelemetryLogLevel.INFO;
        if (level.isMoreSpecificThan(Level.FATAL)) return TelemetryLogLevel.FATAL;
        if (level.isMoreSpecificThan(Level.ERROR)) return TelemetryLogLevel.ERROR;
        if (level.isMoreSpecificThan(Level.WARN)) return TelemetryLogLevel.WARN;
        if (level.isMoreSpecificThan(Level.INFO)) return TelemetryLogLevel.INFO;
        if (level.isMoreSpecificThan(Level.DEBUG)) return TelemetryLogLevel.DEBUG;
        return TelemetryLogLevel.TRACE;
    }

    static String stackTrace(Throwable thrown) {
        BoundedWriter buffer = new BoundedWriter(65536);
        thrown.printStackTrace(new PrintWriter(buffer));
        return buffer.toString();
    }

    private static final class BoundedWriter extends Writer {
        private final StringBuilder value;
        private final int limit;

        BoundedWriter(int limit) {
            this.limit = limit;
            this.value = new StringBuilder(Math.min(1024, limit));
        }

        @Override public void write(char[] chars, int offset, int length) {
            int remaining = limit - value.length();
            if (remaining > 0) value.append(chars, offset, Math.min(remaining, length));
        }
        @Override public void write(String text, int offset, int length) {
            int remaining = limit - value.length();
            if (remaining > 0) {
                int accepted = Math.min(remaining, length);
                value.append(text, offset, offset + accepted);
            }
        }
        @Override public void write(int character) {
            if (value.length() < limit) value.append((char) character);
        }
        @Override public void flush() { }
        @Override public void close() { }
        @Override public String toString() { return value.toString(); }
    }
}
