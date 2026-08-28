package cn.wangz.spark.telemetry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkPluginSignalsSmokeTest {
    private static final Logger LOGGER = LogManager.getLogger("spark.smoke.telemetry");

    @Test
    void runsLocalJobsWithAllSignalsEnabled() {
        // SparkConf loads spark.* JVM properties. To use a remote collector, run this test with
        // -Dspark.telemetry.endpoint=http://<alloy-host>:4317.
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("telemetry-plugin-signals-smoke")
                .set("spark.ui.enabled", "false")
                .set("spark.plugins", SparkTelemetryPlugin.class.getName())
                .set("spark.telemetry.strict", "true")
                .set("spark.telemetry.batch.timeout", "50ms")
                .set("spark.telemetry.export.timeout", "10s")
                .set("spark.telemetry.shutdown.flush-timeout", "3s")
                .set("spark.telemetry.logs.enabled", "true")
                .set("spark.telemetry.logs.capture", "true")
                .set("spark.telemetry.logs.minimum-level", "WARN")
                .set("spark.telemetry.traces.enabled", "true")
                .set("spark.telemetry.traces.task.sample-rate", "1.0");

        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            LOGGER.warn("Starting all-signal Spark telemetry smoke job");

            List<Integer> values = new ArrayList<>();
            for (int value = 1; value <= 40; value++) {
                values.add(value);
            }

            JavaRDD<Integer> transformed = context.parallelize(values, 4)
                    .mapPartitionsWithIndex((partitionId, input) -> {
                        LogManager.getLogger("spark.smoke.telemetry")
                                .warn("Processing partition {}", partitionId);
                        List<Integer> output = new ArrayList<>();
                        input.forEachRemaining(value -> output.add(value * 2));
                        return output.iterator();
                    }, true);

            long divisibleByThree = transformed.filter(value -> value % 3 == 0).count();
            assertEquals(13L, divisibleByThree);

            Map<Integer, Integer> buckets = transformed
                    .mapToPair(value -> new Tuple2<>(value % 4, 1))
                    .reduceByKey(Integer::sum)
                    .collectAsMap();
            int total = buckets.values().stream().mapToInt(Integer::intValue).sum();
            assertEquals(40, total);

            LOGGER.warn("Completed all-signal Spark telemetry smoke job");
        } finally {
            context.stop();
        }
    }
}
