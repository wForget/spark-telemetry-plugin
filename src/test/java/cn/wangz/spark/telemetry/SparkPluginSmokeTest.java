package cn.wangz.spark.telemetry;

import com.codahale.metrics.MetricRegistry;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricProducer;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricRegistry;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SparkPluginSmokeTest {
    @Test
    void runsLocalJobWithPluginLifecycle() {
        SparkConf conf = new SparkConf(false)
                .setMaster("local[2]")
                .setAppName("telemetry-plugin-smoke")
                .set("spark.ui.enabled", "false")
                .set("spark.plugins", UnifiedTelemetryPlugin.class.getName())
                .set("spark.telemetry.strict", "true")
                .set("spark.telemetry.endpoint", "http://127.0.0.1:1")
                .set("spark.telemetry.batch.timeout", "1h")
                .set("spark.telemetry.export.timeout", "100ms")
                .set("spark.telemetry.shutdown.flush-timeout", "500ms")
                .set("spark.telemetry.logs.enabled", "false")
                .set("spark.telemetry.traces.enabled", "false");
        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            long count = context.parallelize(Arrays.asList(1, 2, 3, 4), 2).count();
            assertEquals(4L, count);
            MetricRegistry sparkMetrics = SparkMetricRegistry.current();
            assertFalse(sparkMetrics.getMetrics().isEmpty());
            assertFalse(new SparkMetricProducer(sparkMetrics).produce(Resource.empty()).isEmpty());
        } finally {
            context.stop();
        }
    }
}
