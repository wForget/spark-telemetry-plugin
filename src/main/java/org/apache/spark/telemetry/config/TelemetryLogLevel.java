package org.apache.spark.telemetry.config;

/** Java-friendly log level used at the Scala configuration boundary. */
public enum TelemetryLogLevel {
    TRACE(0), DEBUG(1), INFO(2), WARN(3), ERROR(4), FATAL(5);

    private final int priority;

    TelemetryLogLevel(int priority) {
        this.priority = priority;
    }

    public boolean includes(TelemetryLogLevel actual) {
        return actual.priority >= priority;
    }
}
