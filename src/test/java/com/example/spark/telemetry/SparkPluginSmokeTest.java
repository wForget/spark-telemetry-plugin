package com.example.spark.telemetry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkPluginSmokeTest {
    @Test
    void runsLocalJobWithPluginLifecycle() {
        SparkConf conf = new SparkConf(false)
                .setMaster("local[2]")
                .setAppName("telemetry-plugin-smoke")
                .set("spark.ui.enabled", "false")
                .set("spark.plugins", UnifiedTelemetryPlugin.class.getName())
                .set("spark.telemetry.metrics.enabled", "false")
                .set("spark.telemetry.logs.enabled", "false")
                .set("spark.telemetry.traces.enabled", "false")
                .set("spark.telemetry.profiles.enabled", "false");
        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            long count = context.parallelize(Arrays.asList(1, 2, 3, 4), 2).count();
            assertEquals(4L, count);
        } finally {
            context.stop();
        }
    }
}
