package cn.wangz.spark.telemetry.signal.metrics;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.MetricsSystem;

import java.lang.reflect.Field;

/** Accesses Spark's process-local MetricsSystem registry, which Spark does not expose publicly. */
public final class SparkMetricRegistry {
    private static final Field REGISTRY_FIELD = registryField();

    private SparkMetricRegistry() {
    }

    public static MetricRegistry current() {
        SparkEnv env = SparkEnv.get();
        if (env == null || env.metricsSystem() == null) {
            throw new IllegalStateException("Spark MetricsSystem is not available");
        }
        try {
            return (MetricRegistry) REGISTRY_FIELD.get(env.metricsSystem());
        } catch (IllegalAccessException failure) {
            throw new IllegalStateException("Cannot access Spark MetricsSystem registry", failure);
        }
    }

    private static Field registryField() {
        try {
            Field field = MetricsSystem.class.getDeclaredField("registry");
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Unsupported Spark MetricsSystem implementation", failure);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Cannot open Spark MetricsSystem registry", failure);
        }
    }
}
