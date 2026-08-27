package com.example.spark.telemetry.runtime;

import com.example.spark.telemetry.reliability.PluginSelfMetrics;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.util.Collection;

/** Exporter decorators feeding bounded-cardinality plugin self metrics. */
final class CountingExporters {
    private CountingExporters() {
    }

    static SpanExporter spans(SpanExporter delegate, PluginSelfMetrics metrics) {
        return new CountingSpanExporter(delegate, metrics);
    }
    static LogRecordExporter logs(LogRecordExporter delegate, PluginSelfMetrics metrics) {
        return new CountingLogExporter(delegate, metrics);
    }
    static MetricExporter metrics(MetricExporter delegate, PluginSelfMetrics metrics) {
        return new CountingMetricExporter(delegate, metrics);
    }

    private abstract static class CompletionCounter {
        final PluginSelfMetrics metrics;
        CompletionCounter(PluginSelfMetrics metrics) { this.metrics = metrics; }
        CompletableResultCode count(CompletableResultCode result, final int size, final long started) {
            result.whenComplete(new Runnable() {
                @Override public void run() {
                    long duration = Math.max(0L, System.nanoTime() - started);
                    if (result.isSuccess()) metrics.recordSuccessfulBatch(size, duration);
                    else {
                        metrics.recordFailure();
                        metrics.recordDropped(size);
                    }
                }
            });
            return result;
        }
    }

    private static final class CountingSpanExporter extends CompletionCounter implements SpanExporter {
        private final SpanExporter delegate;
        CountingSpanExporter(SpanExporter delegate, PluginSelfMetrics metrics) { super(metrics); this.delegate = delegate; }
        @Override public CompletableResultCode export(Collection<SpanData> spans) {
            return count(delegate.export(spans), spans.size(), System.nanoTime());
        }
        @Override public CompletableResultCode flush() { return delegate.flush(); }
        @Override public CompletableResultCode shutdown() { return delegate.shutdown(); }
    }

    private static final class CountingLogExporter extends CompletionCounter implements LogRecordExporter {
        private final LogRecordExporter delegate;
        CountingLogExporter(LogRecordExporter delegate, PluginSelfMetrics metrics) { super(metrics); this.delegate = delegate; }
        @Override public CompletableResultCode export(Collection<LogRecordData> logs) {
            return count(delegate.export(logs), logs.size(), System.nanoTime());
        }
        @Override public CompletableResultCode flush() { return delegate.flush(); }
        @Override public CompletableResultCode shutdown() { return delegate.shutdown(); }
    }

    private static final class CountingMetricExporter extends CompletionCounter implements MetricExporter {
        private final MetricExporter delegate;
        CountingMetricExporter(MetricExporter delegate, PluginSelfMetrics metrics) { super(metrics); this.delegate = delegate; }
        @Override public CompletableResultCode export(Collection<MetricData> metrics) {
            return count(delegate.export(metrics), metrics.size(), System.nanoTime());
        }
        @Override public AggregationTemporality getAggregationTemporality(InstrumentType type) {
            return delegate.getAggregationTemporality(type);
        }
        @Override public Aggregation getDefaultAggregation(InstrumentType type) {
            return delegate.getDefaultAggregation(type);
        }
        @Override public MemoryMode getMemoryMode() { return delegate.getMemoryMode(); }
        @Override public CompletableResultCode flush() { return delegate.flush(); }
        @Override public CompletableResultCode shutdown() { return delegate.shutdown(); }
    }
}
