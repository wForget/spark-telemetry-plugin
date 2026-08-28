package com.example.spark.telemetry.instrumentation;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.example.spark.telemetry.reliability.PluginSelfMetrics;
import com.example.spark.telemetry.runtime.TelemetryRuntime;

/** Exposes the same bounded self-observability state through Spark's plugin metric source. */
public final class SparkSelfMetrics {
    private static final String[] SIGNALS = {"metrics", "logs", "traces"};

    private SparkSelfMetrics() {
    }

    public static void register(MetricRegistry registry, TelemetryRuntime runtime) {
        for (String signal : SIGNALS) registerSignal(registry, signal, runtime.selfMetrics(signal));
    }

    private static void registerSignal(
            MetricRegistry registry,
            String signal,
            final PluginSelfMetrics metrics) {
        String prefix = "telemetry." + signal + ".";
        registry.register(prefix + "events_received_total", new Gauge<Long>() {
            @Override public Long getValue() { return metrics.snapshot().eventsReceived(); }
        });
        registry.register(prefix + "events_exported_total", new Gauge<Long>() {
            @Override public Long getValue() { return metrics.snapshot().eventsExported(); }
        });
        registry.register(prefix + "events_dropped_total", new Gauge<Long>() {
            @Override public Long getValue() { return metrics.snapshot().eventsDropped(); }
        });
        registry.register(prefix + "export_failures_total", new Gauge<Long>() {
            @Override public Long getValue() { return metrics.snapshot().exportFailures(); }
        });
    }
}
