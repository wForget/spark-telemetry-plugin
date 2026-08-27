package com.example.spark.telemetry.instrumentation;

import com.example.spark.telemetry.runtime.LogPipeline;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/** Programmatically attaches without replacing Spark's existing appenders. */
public final class Log4j2TelemetryBridge implements AutoCloseable {
    private final LoggerContext context;
    private final LoggerConfig root;
    private final TelemetryLogAppender appender;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Log4j2TelemetryBridge(
            LoggerContext context,
            LoggerConfig root,
            TelemetryLogAppender appender) {
        this.context = context;
        this.root = root;
        this.appender = appender;
    }

    public static Log4j2TelemetryBridge install(String instanceName, LogPipeline pipeline) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        LoggerConfig root = configuration.getRootLogger();
        String name = "SparkTelemetry-" + instanceName;
        TelemetryLogAppender appender = new TelemetryLogAppender(name, pipeline);
        appender.start();
        configuration.addAppender(appender);
        root.addAppender(appender, null, null);
        context.updateLoggers();
        return new Log4j2TelemetryBridge(context, root, appender);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        root.removeAppender(appender.getName());
        context.getConfiguration().getAppenders().remove(appender.getName());
        context.updateLoggers();
        appender.stop();
    }
}
