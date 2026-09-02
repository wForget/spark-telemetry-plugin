package cn.wangz.spark.telemetry.runtime;

import com.codahale.metrics.MetricRegistry;
import cn.wangz.spark.telemetry.signal.logs.LogPipeline;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricProducer;
import cn.wangz.spark.telemetry.signal.profiles.ProfileLifecycle;
import cn.wangz.spark.telemetry.signal.profiles.ProfilePipeline;
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
import io.opentelemetry.sdk.trace.SpanLimits;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.spark.telemetry.config.TelemetryConfig;

/**
 * One Driver or Executor process-local runtime. Providers are deliberately not installed globally.
 */
public final class TelemetryRuntime implements AutoCloseable {
    private static final String INSTRUMENTATION_SCOPE = "spark-telemetry-plugin";
    private static final org.apache.logging.log4j.Logger LOG =
            LogManager.getLogger(TelemetryRuntime.class);
    private static final AtomicBoolean PROFILE_FAILURE_REPORTED = new AtomicBoolean();

    private final TelemetryConfig config;
    private final SdkMeterProvider meterProvider;
    private final SdkTracerProvider tracerProvider;
    private final SdkLoggerProvider loggerProvider;
    private final TracePipeline traces;
    private final LogPipeline logs;
    private final ProfileLifecycle profiles;
    private final AtomicReference<State> state = new AtomicReference<State>(State.RUNNING);

    private TelemetryRuntime(
            TelemetryConfig config,
            SdkMeterProvider meterProvider,
            SdkTracerProvider tracerProvider,
            SdkLoggerProvider loggerProvider,
            TracePipeline traces,
            LogPipeline logs,
            ProfileLifecycle profiles) {
        this.config = config;
        this.meterProvider = meterProvider;
        this.tracerProvider = tracerProvider;
        this.loggerProvider = loggerProvider;
        this.traces = traces;
        this.logs = logs;
        this.profiles = profiles;
    }

    public static TelemetryRuntime create(
            TelemetryConfig config, ResourceIdentity identity, MetricRegistry sparkMetrics) {
        return create(config, identity, sparkMetrics, true);
    }

    public static TelemetryRuntime create(
            TelemetryConfig config,
            ResourceIdentity identity,
            MetricRegistry sparkMetrics,
            boolean profilesAllowed) {
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
                new LogPipeline(logger, config.minimumLogLevel()),
                buildProfilePipeline(config, identity, profilesAllowed));
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
            Thread profileShutdown = null;
            if (profiles != null) {
                profileShutdown = new Thread(new Runnable() {
                    @Override public void run() {
                        profiles.close(remaining(deadline));
                    }
                }, "spark-telemetry-profile-shutdown");
                profileShutdown.setDaemon(true);
                profileShutdown.start();
            }
            providerShutdown.start();
            joinUntil(providerShutdown, deadline);
            joinUntil(profileShutdown, deadline);
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
            builder.registerMetricProducer(
                    new SparkMetricProducer(sparkMetrics, identity.metricAttributes()));
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
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
                .setResource(identity.detailedResource())
                // Bounds exception.stacktrace produced by Span.recordException(Throwable).
                .setSpanLimits(SpanLimits.builder().setMaxAttributeValueLength(65536).build());
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

    private static ProfileLifecycle buildProfilePipeline(
            TelemetryConfig config, ResourceIdentity identity, boolean profilesAllowed) {
        if (!profilesAllowed || !config.profilesEnabled()) return null;
        try {
            return ProfilePipeline.startAsync(config, identity);
        } catch (RuntimeException failure) {
            reportProfileFailure(failure);
            return null;
        } catch (LinkageError failure) {
            reportProfileFailure(failure);
            return null;
        }
    }

    private static void reportProfileFailure(Throwable failure) {
        if (PROFILE_FAILURE_REPORTED.compareAndSet(false, true)) {
            LOG.warn("Pyroscope is unavailable; profiling is disabled while other telemetry remains active: {}",
                    failure.toString());
        }
    }

    private static void flush(io.opentelemetry.sdk.common.CompletableResultCode result, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining > 0L) result.join(remaining, TimeUnit.NANOSECONDS);
    }

    private static Duration remaining(long deadline) {
        return Duration.ofNanos(Math.max(0L, deadline - System.nanoTime()));
    }

    private static void joinUntil(Thread thread, long deadline) {
        if (thread == null) return;
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
