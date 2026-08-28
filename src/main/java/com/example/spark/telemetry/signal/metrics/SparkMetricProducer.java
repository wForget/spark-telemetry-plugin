package com.example.spark.telemetry.signal.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Adapts the process-local Spark {@link org.apache.spark.metrics.MetricsSystem} registry to OTel.
 *
 * <p>The producer owns no instruments or metric state. It reads the registry on every collection,
 * so metrics registered by Spark after plugin initialization are included automatically.</p>
 */
public final class SparkMetricProducer implements MetricProducer {
    private static final InstrumentationScopeInfo SCOPE =
            InstrumentationScopeInfo.create("org.apache.spark.metrics.MetricsSystem");
    private static final Attributes NO_ATTRIBUTES = Attributes.empty();
    private static final double NANOS_PER_MILLISECOND = 1_000_000.0d;

    private final MetricRegistry registry;
    private final long processStartEpochNanos;

    public SparkMetricProducer(MetricRegistry registry) {
        if (registry == null) throw new IllegalArgumentException("registry must not be null");
        this.registry = registry;
        this.processStartEpochNanos = processStartEpochNanos();
    }

    @Override
    public Collection<MetricData> produce(Resource resource) {
        long now = System.currentTimeMillis() * 1_000_000L;
        List<MetricData> result = new ArrayList<MetricData>();
        for (Map.Entry<String, Metric> entry : registry.getMetrics().entrySet()) {
            try {
                append(result, resource, entry.getKey(), entry.getValue(), now);
            } catch (RuntimeException ignored) {
                // A broken application or Spark gauge must not suppress the rest of the registry.
            } catch (LinkageError ignored) {
                // Spark supplies Dropwizard; tolerate an unexpected runtime ABI mismatch.
            }
        }
        return result;
    }

    private void append(
            List<MetricData> target,
            Resource resource,
            String name,
            Metric metric,
            long now) {
        if (metric instanceof Timer) {
            Timer timer = (Timer) metric;
            appendMetered(target, resource, name, timer, now);
            appendSnapshot(target, resource, name + ".duration", timer, now,
                    1.0d / NANOS_PER_MILLISECOND, "ms");
        } else if (metric instanceof Histogram) {
            Histogram histogram = (Histogram) metric;
            target.add(longSum(resource, name + ".count", histogram.getCount(), now));
            appendSnapshot(target, resource, name, histogram, now, 1.0d, "1");
        } else if (metric instanceof Meter) {
            appendMetered(target, resource, name, (Meter) metric, now);
        } else if (metric instanceof Counter) {
            // Dropwizard counters can be decremented, so representing them as sums is unsafe.
            target.add(longGauge(resource, name, ((Counter) metric).getCount(), now, "1"));
        } else if (metric instanceof Gauge) {
            appendGauge(target, resource, name, (Gauge<?>) metric, now);
        }
    }

    private void appendGauge(
            List<MetricData> target,
            Resource resource,
            String name,
            Gauge<?> gauge,
            long now) {
        Object value = gauge.getValue();
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            target.add(longGauge(resource, name, ((Number) value).longValue(), now, "1"));
        } else if (value instanceof Number) {
            double numeric = ((Number) value).doubleValue();
            if (Double.isFinite(numeric)) {
                target.add(doubleGauge(resource, name, numeric, now, "1"));
            }
        } else if (value instanceof Boolean) {
            target.add(longGauge(resource, name, ((Boolean) value).booleanValue() ? 1L : 0L, now, "1"));
        }
    }

    private void appendMetered(
            List<MetricData> target,
            Resource resource,
            String name,
            Metered meter,
            long now) {
        target.add(longSum(resource, name + ".count", meter.getCount(), now));
        target.add(doubleGauge(resource, name + ".rate.mean", meter.getMeanRate(), now, "{event}/s"));
        target.add(doubleGauge(resource, name + ".rate.m1", meter.getOneMinuteRate(), now, "{event}/s"));
        target.add(doubleGauge(resource, name + ".rate.m5", meter.getFiveMinuteRate(), now, "{event}/s"));
        target.add(doubleGauge(resource, name + ".rate.m15", meter.getFifteenMinuteRate(), now, "{event}/s"));
    }

    private void appendSnapshot(
            List<MetricData> target,
            Resource resource,
            String name,
            Sampling sampling,
            long now,
            double scale,
            String unit) {
        Snapshot snapshot = sampling.getSnapshot();
        target.add(doubleGauge(resource, name + ".min", snapshot.getMin() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".max", snapshot.getMax() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".mean", snapshot.getMean() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".stddev", snapshot.getStdDev() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p50", snapshot.getMedian() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p75", snapshot.get75thPercentile() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p95", snapshot.get95thPercentile() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p98", snapshot.get98thPercentile() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p99", snapshot.get99thPercentile() * scale, now, unit));
        target.add(doubleGauge(resource, name + ".p999", snapshot.get999thPercentile() * scale, now, unit));
    }

    private MetricData longGauge(Resource resource, String name, long value, long now, String unit) {
        LongPointData point = ImmutableLongPointData.create(0L, now, NO_ATTRIBUTES, value);
        return ImmutableMetricData.createLongGauge(
                resource, SCOPE, name, "Spark MetricsSystem registry metric", unit,
                ImmutableGaugeData.create(Collections.singletonList(point)));
    }

    private MetricData doubleGauge(
            Resource resource, String name, double value, long now, String unit) {
        DoublePointData point = ImmutableDoublePointData.create(0L, now, NO_ATTRIBUTES, value);
        return ImmutableMetricData.createDoubleGauge(
                resource, SCOPE, name, "Spark MetricsSystem registry metric", unit,
                ImmutableGaugeData.create(Collections.singletonList(point)));
    }

    private MetricData longSum(Resource resource, String name, long value, long now) {
        LongPointData point = ImmutableLongPointData.create(
                processStartEpochNanos, now, NO_ATTRIBUTES, value);
        return ImmutableMetricData.createLongSum(
                resource, SCOPE, name, "Spark MetricsSystem registry metric", "{event}",
                ImmutableSumData.create(
                        true, AggregationTemporality.CUMULATIVE,
                        Collections.singletonList(point)));
    }

    private static long processStartEpochNanos() {
        try {
            return ManagementFactory.getRuntimeMXBean().getStartTime() * 1_000_000L;
        } catch (RuntimeException unavailable) {
            return System.currentTimeMillis() * 1_000_000L;
        }
    }
}
