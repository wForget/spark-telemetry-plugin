package com.example.spark.telemetry.runtime;

import com.example.spark.telemetry.signal.logs.LogPipeline;
import com.example.spark.telemetry.signal.metrics.MetricPipeline;
import com.example.spark.telemetry.signal.traces.TaskFilteringSpanProcessor;
import com.example.spark.telemetry.signal.traces.TaskSpanHandle;
import com.example.spark.telemetry.signal.traces.TracePipeline;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.SdkLoggerProviderBuilder;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.telemetry.config.TelemetryConfig;

/**
 * One Driver or Executor process-local runtime. Providers are deliberately not installed globally.
 */
public final class TelemetryRuntime implements AutoCloseable {
    private static final String INSTRUMENTATION_SCOPE = "spark-unified-telemetry-plugin";

    private final TelemetryConfig config;
    private final SdkMeterProvider meterProvider;
    private final SdkTracerProvider tracerProvider;
    private final SdkLoggerProvider loggerProvider;
    private final MetricPipeline metrics;
    private final TracePipeline traces;
    private final LogPipeline logs;
    private final AtomicReference<State> state = new AtomicReference<State>(State.RUNNING);

    private TelemetryRuntime(
            TelemetryConfig config,
            SdkMeterProvider meterProvider,
            SdkTracerProvider tracerProvider,
            SdkLoggerProvider loggerProvider,
            MetricPipeline metrics,
            TracePipeline traces,
            LogPipeline logs) {
        this.config = config;
        this.meterProvider = meterProvider;
        this.tracerProvider = tracerProvider;
        this.loggerProvider = loggerProvider;
        this.metrics = metrics;
        this.traces = traces;
        this.logs = logs;
    }

    public static TelemetryRuntime create(TelemetryConfig config, ResourceIdentity identity) {
        SdkMeterProvider meterProvider = buildMeterProvider(config, identity);
        SdkTracerProvider tracerProvider = buildTracerProvider(config, identity);
        SdkLoggerProvider loggerProvider = buildLoggerProvider(config, identity);
        Meter meter = meterProvider.get(INSTRUMENTATION_SCOPE);
        Tracer tracer = tracerProvider.get(INSTRUMENTATION_SCOPE);
        Logger logger = loggerProvider.get(INSTRUMENTATION_SCOPE);
        return new TelemetryRuntime(
                config,
                meterProvider,
                tracerProvider,
                loggerProvider,
                new MetricPipeline(meter),
                new TracePipeline(tracer),
                new LogPipeline(logger, config.minimumLogLevel()));
    }

    public LogPipeline logs() { return logs; }
    public boolean isRunning() { return state.get() == State.RUNNING; }

    public void applicationStarted(long epochMillis) {
        runSafely(new Action() { @Override public void run() { traces.applicationStarted(epochMillis); } });
    }
    public void applicationEnded(final long epochMillis) {
        runSafely(new Action() { @Override public void run() { traces.applicationEnded(epochMillis); } });
    }
    public void jobStarted(final int jobId, final int[] stageIds, final long epochMillis) {
        runSafely(new Action() { @Override public void run() {
            metrics.jobStarted();
            traces.jobStarted(jobId, stageIds, epochMillis);
        }});
    }
    public void jobEnded(
            final int jobId,
            final long startMillis,
            final long endMillis,
            final String outcome,
            final String failure) {
        runSafely(new Action() { @Override public void run() {
            metrics.jobEnded(Math.max(0L, endMillis - startMillis), outcome);
            traces.jobEnded(jobId, endMillis, outcome, failure);
        }});
    }
    public void stageStarted(final int stageId, final int attempt, final long epochMillis) {
        runSafely(new Action() { @Override public void run() {
            metrics.stageStarted();
            traces.stageStarted(stageId, attempt, epochMillis);
        }});
    }
    public void stageEnded(
            final int stageId,
            final int attempt,
            final long startMillis,
            final long endMillis,
            final String outcome,
            final String failure) {
        runSafely(new Action() { @Override public void run() {
            metrics.stageEnded(Math.max(0L, endMillis - startMillis), outcome);
            traces.stageEnded(stageId, attempt, endMillis, outcome, failure);
        }});
    }
    public void executorAdded() {
        runSafely(new Action() { @Override public void run() { metrics.executorAdded(); } });
    }
    public void executorRemoved() {
        runSafely(new Action() { @Override public void run() { metrics.executorRemoved(); } });
    }
    public void taskMetricStarted() {
        runSafely(new Action() { @Override public void run() { metrics.taskStarted(); } });
    }
    public void taskMetricEnded(final long durationMillis, final String outcome) {
        runSafely(new Action() { @Override public void run() {
            metrics.taskEnded(TimeUnit.MILLISECONDS.toNanos(Math.max(0L, durationMillis)), outcome);
        }});
    }
    public TaskSpanHandle taskTraceStarted(
            long taskAttemptId,
            int stageId,
            int stageAttempt,
            int partitionId,
            int attemptNumber,
            long startEpochNanos) {
        if (state.get() != State.RUNNING || !config.tracesEnabled()) return null;
        try {
            return traces.taskStarted(taskAttemptId, stageId, stageAttempt, partitionId,
                    attemptNumber, startEpochNanos);
        } catch (RuntimeException ignored) {
            return null;
        } catch (LinkageError ignored) {
            return null;
        }
    }
    public void taskTraceEnded(
            final TaskSpanHandle handle,
            final long endEpochNanos,
            final String outcome,
            final String failure,
            final boolean retain) {
        runSafely(new Action() { @Override public void run() {
            traces.taskEnded(handle, endEpochNanos, outcome, failure, retain);
        }});
    }

    public void close(Duration timeout) {
        if (!state.compareAndSet(State.RUNNING, State.CLOSING)) return;
        final long deadline = System.nanoTime() + Math.max(0L, timeout.toNanos());
        try {
            traces.close(System.currentTimeMillis());
            Thread providerShutdown = new Thread(new Runnable() {
                @Override public void run() {
                    // Some SDK provider shutdown implementations synchronously await an in-flight
                    // export before returning their CompletableResultCode. Keep that wait off Spark's
                    // shutdown thread and enforce the plugin's single absolute deadline externally.
                    flush(meterProvider.forceFlush(), deadline);
                    flush(loggerProvider.forceFlush(), deadline);
                    flush(tracerProvider.forceFlush(), deadline);
                    flush(loggerProvider.shutdown(), deadline);
                    flush(tracerProvider.shutdown(), deadline);
                    flush(meterProvider.shutdown(), deadline);
                }
            }, "spark-telemetry-provider-shutdown");
            providerShutdown.setDaemon(true);
            providerShutdown.start();
            joinUntil(providerShutdown, deadline);
        } catch (RuntimeException ignored) {
            // A partially initialized or failing SDK must not block Spark shutdown.
        } catch (LinkageError ignored) {
            // A provided-dependency ABI mismatch is also fail-open.
        } finally {
            state.set(State.CLOSED);
        }
    }

    @Override
    public void close() {
        close(config.shutdownFlushTimeout());
    }

    private void runSafely(Action action) {
        if (state.get() != State.RUNNING) return;
        try {
            action.run();
        } catch (RuntimeException ignored) {
            // Telemetry is fail-open by contract.
        } catch (LinkageError ignored) {
            // Do not let a provided-dependency ABI mismatch fail Spark callbacks.
        }
    }

    private static SdkMeterProvider buildMeterProvider(
            TelemetryConfig config, ResourceIdentity identity) {
        SdkMeterProviderBuilder builder = SdkMeterProvider.builder().setResource(identity.metricResource());
        if (config.metricsEnabled()) {
            OtlpHttpMetricExporter otlpExporter = OtlpHttpMetricExporter.builder()
                    .setEndpoint(config.otlpSignalEndpoint("metrics"))
                    .setTimeout(config.exportTimeout())
                    .build();
            builder.registerMetricReader(PeriodicMetricReader.builder(otlpExporter)
                    .setInterval(config.batchTimeout())
                    .build());
        }
        return builder.build();
    }

    private static SdkTracerProvider buildTracerProvider(
            TelemetryConfig config, ResourceIdentity identity) {
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder().setResource(identity.detailedResource());
        if (config.tracesEnabled()) {
            OtlpHttpSpanExporter otlpExporter = OtlpHttpSpanExporter.builder()
                    .setEndpoint(config.otlpSignalEndpoint("traces"))
                    .setTimeout(config.exportTimeout())
                    .build();
            BatchSpanProcessor batchProcessor = BatchSpanProcessor.builder(otlpExporter)
                    .setMaxQueueSize(config.tracesQueueCapacity())
                    .setMaxExportBatchSize(Math.min(config.batchMaxSize(), config.tracesQueueCapacity()))
                    .setScheduleDelay(config.batchTimeout())
                    .setExporterTimeout(config.exportTimeout())
                    .build();
            builder.addSpanProcessor(new TaskFilteringSpanProcessor(batchProcessor));
        }
        return builder.build();
    }

    private static SdkLoggerProvider buildLoggerProvider(
            TelemetryConfig config, ResourceIdentity identity) {
        SdkLoggerProviderBuilder builder = SdkLoggerProvider.builder().setResource(identity.detailedResource());
        if (config.logsEnabled()) {
            OtlpHttpLogRecordExporter otlpExporter = OtlpHttpLogRecordExporter.builder()
                    .setEndpoint(config.otlpSignalEndpoint("logs"))
                    .setTimeout(config.exportTimeout())
                    .build();
            builder.addLogRecordProcessor(BatchLogRecordProcessor.builder(otlpExporter)
                    .setMaxQueueSize(config.logsQueueCapacity())
                    .setMaxExportBatchSize(Math.min(config.batchMaxSize(), config.logsQueueCapacity()))
                    .setScheduleDelay(config.batchTimeout())
                    .setExporterTimeout(config.exportTimeout())
                    .build());
        }
        return builder.build();
    }

    private static void flush(io.opentelemetry.sdk.common.CompletableResultCode result, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining > 0L) result.join(remaining, TimeUnit.NANOSECONDS);
    }

    private static void joinUntil(Thread thread, long deadline) {
        boolean interrupted = false;
        try {
            while (thread.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) return;
                try {
                    TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
                } catch (InterruptedException interruption) {
                    interrupted = true;
                    return;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private interface Action { void run(); }
    private enum State { RUNNING, CLOSING, CLOSED }
}
