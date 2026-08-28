package cn.wangz.spark.telemetry.runtime;

import com.codahale.metrics.MetricRegistry;
import cn.wangz.spark.telemetry.signal.logs.LogPipeline;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricProducer;
import cn.wangz.spark.telemetry.signal.traces.TaskFilteringSpanProcessor;
import cn.wangz.spark.telemetry.signal.traces.TracePipeline;
import cn.wangz.spark.telemetry.signal.traces.TraceSink;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
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
    private final TracePipeline traces;
    private final LogPipeline logs;
    private final AtomicReference<State> state = new AtomicReference<State>(State.RUNNING);

    private TelemetryRuntime(
            TelemetryConfig config,
            SdkMeterProvider meterProvider,
            SdkTracerProvider tracerProvider,
            SdkLoggerProvider loggerProvider,
            TracePipeline traces,
            LogPipeline logs) {
        this.config = config;
        this.meterProvider = meterProvider;
        this.tracerProvider = tracerProvider;
        this.loggerProvider = loggerProvider;
        this.traces = traces;
        this.logs = logs;
    }

    public static TelemetryRuntime create(
            TelemetryConfig config, ResourceIdentity identity, MetricRegistry sparkMetrics) {
        SdkMeterProvider meterProvider = buildMeterProvider(config, identity, sparkMetrics);
        SdkTracerProvider tracerProvider = buildTracerProvider(config, identity);
        SdkLoggerProvider loggerProvider = buildLoggerProvider(config, identity);
        Tracer tracer = tracerProvider.get(INSTRUMENTATION_SCOPE);
        Logger logger = loggerProvider.get(INSTRUMENTATION_SCOPE);
        return new TelemetryRuntime(
                config,
                meterProvider,
                tracerProvider,
                loggerProvider,
                new TracePipeline(tracer, config.tracesEnabled()),
                new LogPipeline(logger, config.minimumLogLevel()));
    }

    public LogPipeline logs() { return logs; }
    public TraceSink traces() { return traces; }
    public boolean isRunning() { return state.get() == State.RUNNING; }

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

    private static SdkMeterProvider buildMeterProvider(
            TelemetryConfig config, ResourceIdentity identity, MetricRegistry sparkMetrics) {
        SdkMeterProviderBuilder builder = SdkMeterProvider.builder().setResource(identity.metricResource());
        if (config.metricsEnabled() && sparkMetrics != null) {
            builder.registerMetricProducer(new SparkMetricProducer(sparkMetrics));
            OtlpGrpcMetricExporter otlpExporter = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(config.endpoint())
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
            OtlpGrpcSpanExporter otlpExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(config.endpoint())
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
            OtlpGrpcLogRecordExporter otlpExporter = OtlpGrpcLogRecordExporter.builder()
                    .setEndpoint(config.endpoint())
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

    private enum State { RUNNING, CLOSING, CLOSED }
}
