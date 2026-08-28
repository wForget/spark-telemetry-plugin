package com.example.spark.telemetry.profile;

import com.example.spark.telemetry.reliability.BatchProcessor;
import com.example.spark.telemetry.reliability.BoundedSignalQueue;
import com.example.spark.telemetry.reliability.CircuitBreaker;
import com.example.spark.telemetry.reliability.PluginSelfMetrics;
import com.example.spark.telemetry.reliability.RetryPolicy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.telemetry.config.TelemetryConfig;

/** One queue item per complete profile window; overload skips the incoming window. */
public final class ProfilePipeline implements AutoCloseable, ProfileCollector.Sink {
    private final ProfileExporter exporter;
    private final BoundedSignalQueue<ProfileBatch> queue;
    private final BatchProcessor<ProfileBatch> processor;
    private final RetryPolicy retryPolicy;
    private final CircuitBreaker circuitBreaker;
    private final PluginSelfMetrics selfMetrics;

    public ProfilePipeline(TelemetryConfig config, PluginSelfMetrics selfMetrics) {
        this(new PyroscopeProfileExporter(config.profileEndpoint(), config.exportTimeout()),
                config.profilesQueueCapacity(), config.batchTimeout(), selfMetrics);
    }

    ProfilePipeline(
            ProfileExporter exporter,
            int capacity,
            Duration flushInterval,
            PluginSelfMetrics selfMetrics) {
        this.exporter = exporter;
        this.selfMetrics = selfMetrics;
        this.queue = new BoundedSignalQueue<ProfileBatch>(capacity);
        this.selfMetrics.setQueueGauge(PluginSelfMetrics.queueGauge(queue));
        this.retryPolicy = new RetryPolicy(100L, 2000L, 2.0d, 0.2d, 3, 5000L);
        this.circuitBreaker = new CircuitBreaker(5, 30L, TimeUnit.SECONDS);
        this.processor = new BatchProcessor<ProfileBatch>(
                queue, 1, Math.max(1L, flushInterval.toMillis()), TimeUnit.MILLISECONDS,
                new BatchProcessor.Exporter<ProfileBatch>() {
                    @Override public void export(List<ProfileBatch> batch) throws Exception {
                        exportWithRetry(batch.get(0));
                    }
                }, selfMetrics.batchListener(), null);
        this.processor.start();
    }

    @Override
    public boolean offer(ProfileBatch batch) {
        selfMetrics.recordReceived();
        boolean accepted = processor.offerFromBackground(batch);
        if (!accepted) selfMetrics.recordDropped();
        return accepted;
    }

    public boolean close(Duration timeout) {
        boolean drained = processor.shutdown(Math.max(0L, timeout.toNanos()), TimeUnit.NANOSECONDS);
        exporter.close();
        return drained;
    }

    @Override
    public void close() {
        close(Duration.ZERO);
    }

    private void exportWithRetry(ProfileBatch batch) throws Exception {
        if (!circuitBreaker.tryAcquire()) throw new IOExceptionMarker("profile circuit is open");
        long started = System.nanoTime();
        int retryAttempt = 0;
        while (true) {
            ProfileExporter.ExportResult result = exporter.export(batch);
            if (result.success()) {
                circuitBreaker.recordSuccess();
                return;
            }
            if (!result.retryable()) {
                circuitBreaker.recordFailure();
                throw new IOExceptionMarker(result.message());
            }
            retryAttempt++;
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            long delay = retryPolicy.nextDelayMillis(retryAttempt, elapsedMillis);
            if (delay < 0L) {
                circuitBreaker.recordFailure();
                throw new IOExceptionMarker(result.message());
            }
            selfMetrics.recordRetry();
            Thread.sleep(delay);
        }
    }

    private static final class IOExceptionMarker extends Exception {
        IOExceptionMarker(String message) { super(message); }
    }
}
