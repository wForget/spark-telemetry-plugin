package cn.wangz.spark.telemetry;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkPluginSignalsSmokeTest {
    private static final Logger LOGGER = LogManager.getLogger("spark.smoke.telemetry");
    private static final int LARGE_OBJECT_BYTES = 4 * 1024 * 1024;
    private static final int LARGE_OBJECT_COUNT = 8;
    private static final AtomicInteger INTENTIONAL_FAILURES = new AtomicInteger();
    private static final AtomicInteger RETRIED_SUCCESSES = new AtomicInteger();

    @Test
    void coversSuccessFailureSkewShuffleAndMemorySignals() {
        INTENTIONAL_FAILURES.set(0);
        RETRIED_SUCCESSES.set(0);

        // SparkConf loads spark.* JVM properties. To use a remote collector, run this test with
        // -Dspark.telemetry.endpoint=http://<alloy-host>:4317.
        SparkConf conf = new SparkConf()
                .setMaster("local[2,2]")
                .setAppName("telemetry-plugin-signals-smoke")
                .set("spark.ui.enabled", "false")
                .set("spark.plugins", SparkTelemetryPlugin.class.getName())
                .set("spark.task.maxFailures", "2")
                // Local mode refreshes the Driver ExecutorMetricsSource from the driver heartbeat;
                // the executor polling interval alone only accelerates non-local executors.
                .set("spark.executor.heartbeatInterval", "100ms")
                .set("spark.executor.metrics.pollingInterval", "100ms")
                .set("spark.executor.processTreeMetrics.enabled", "true")
                .set("spark.telemetry.strict", "true")
                .set("spark.telemetry.batch.timeout", "50ms")
                .set("spark.telemetry.export.timeout", "10s")
                .set("spark.telemetry.shutdown.flush-timeout", "3s")
                .set("spark.telemetry.logs.enabled", "true")
                .set("spark.telemetry.logs.capture", "true")
                .set("spark.telemetry.logs.minimum-level", "WARN")
                .set("spark.telemetry.traces.enabled", "true")
                .set("spark.telemetry.traces.task.sample-rate", "1.0")
                .set("spark.telemetry.traces.slow-task-threshold", "300ms");

        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            LOGGER.warn("Starting all-signal Spark telemetry smoke scenarios");
            runSuccessfulTransformations(context);
            runFailureAndRetry(context);
            runSkewedTasks(context);
            runSkewedShuffle(context);
            runLargeObjectMemoryPressure(context);
            sleepMillis(250L);
            LOGGER.warn("Completed all-signal Spark telemetry smoke scenarios");
        } finally {
            context.stop();
        }
    }

    private static void runSuccessfulTransformations(JavaSparkContext context) {
        LOGGER.warn("Scenario: successful transformations");
        List<Integer> values = integers(1, 40);
        JavaRDD<Integer> transformed = context.parallelize(values, 4)
                .mapPartitionsWithIndex((partitionId, input) -> {
                    LOGGER.warn("Processing successful partition {}", partitionId);
                    List<Integer> output = new ArrayList<Integer>();
                    input.forEachRemaining(value -> output.add(value * 2));
                    return output.iterator();
                }, true);

        assertEquals(13L, transformed.filter(value -> value % 3 == 0).count());
    }

    private static void runFailureAndRetry(JavaSparkContext context) {
        LOGGER.warn("Scenario: one task fails and succeeds on retry");
        long count = context.parallelize(integers(1, 40), 4)
                .mapPartitionsWithIndex((partitionId, input) -> {
                    int attempt = TaskContext.get().attemptNumber();
                    if (partitionId == 0 && attempt == 0) {
                        INTENTIONAL_FAILURES.incrementAndGet();
                        LOGGER.warn("Injecting task failure for partition {} attempt {}", partitionId, attempt);
                        throw new IllegalStateException("intentional smoke-test task failure");
                    }
                    if (partitionId == 0) {
                        RETRIED_SUCCESSES.incrementAndGet();
                        LOGGER.warn("Recovered partition {} on attempt {}", partitionId, attempt);
                    }
                    List<Integer> output = new ArrayList<Integer>();
                    input.forEachRemaining(output::add);
                    return output.iterator();
                }, true)
                .count();

        assertEquals(40L, count);
        assertEquals(1, INTENTIONAL_FAILURES.get());
        assertEquals(1, RETRIED_SUCCESSES.get());
    }

    private static void runSkewedTasks(JavaSparkContext context) {
        LOGGER.warn("Scenario: one deliberately slow partition");
        long count = context.parallelize(integers(1, 80), 4)
                .mapPartitionsWithIndex((partitionId, input) -> {
                    long delayMillis = partitionId == 0 ? 700L : 25L;
                    LOGGER.warn("Running partition {} with {} ms delay", partitionId, delayMillis);
                    sleepMillis(delayMillis);
                    List<Integer> output = new ArrayList<Integer>();
                    input.forEachRemaining(output::add);
                    return output.iterator();
                }, true)
                .count();

        assertEquals(80L, count);
    }

    private static void runSkewedShuffle(JavaSparkContext context) {
        LOGGER.warn("Scenario: skewed wide dependency and shuffle");
        List<Integer> values = integers(0, 19_999);
        Map<Integer, Integer> buckets = context.parallelize(values, 8)
                .mapToPair(value -> new Tuple2<Integer, Integer>(value < 18_000 ? 0 : value, 1))
                .reduceByKey(Integer::sum)
                .collectAsMap();

        int total = buckets.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(20_000, total);
        assertEquals(18_000, buckets.get(0).intValue());
    }

    private static void runLargeObjectMemoryPressure(JavaSparkContext context) {
        LOGGER.warn("Scenario: persist {} objects of {} bytes", LARGE_OBJECT_COUNT, LARGE_OBJECT_BYTES);
        JavaRDD<byte[]> largeObjects = context
                .parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7), 4)
                .map(index -> {
                    byte[] payload = new byte[LARGE_OBJECT_BYTES];
                    Arrays.fill(payload, (byte) (index + 1));
                    return payload;
                })
                .persist(StorageLevel.MEMORY_ONLY());
        try {
            assertEquals(LARGE_OBJECT_COUNT, largeObjects.count());
            // Keep cached blocks alive across several Spark and OTel metric collections.
            sleepMillis(500L);

            MetricRegistry registry = SparkMetricRegistry.current();
            long heap = executorMetric(registry, "JVMHeapMemory");
            long nonHeap = executorMetric(registry, "JVMOffHeapMemory");
            long storage = executorMetric(registry, "OnHeapStorageMemory");
            LOGGER.warn(
                    "ExecutorMetrics snapshot: heap={} nonHeap={} onHeapStorage={}",
                    heap, nonHeap, storage);

            assertTrue(heap > 0L, "JVMHeapMemory should be refreshed before shutdown");
            assertTrue(nonHeap > 0L, "JVMOffHeapMemory should be refreshed before shutdown");
            assertTrue(storage > 0L, "persisted large objects should consume Spark storage memory");
        } finally {
            largeObjects.unpersist(false);
        }
    }

    private static long executorMetric(MetricRegistry registry, String metricName) {
        String suffix = ".ExecutorMetrics." + metricName;
        for (Map.Entry<String, Metric> entry : registry.getMetrics().entrySet()) {
            if (entry.getKey().endsWith(suffix) && entry.getValue() instanceof Gauge) {
                Object value = ((Gauge<?>) entry.getValue()).getValue();
                if (value instanceof Number) return ((Number) value).longValue();
            }
        }
        throw new AssertionError("ExecutorMetrics gauge not found: " + metricName);
    }

    private static List<Integer> integers(int first, int lastInclusive) {
        List<Integer> values = new ArrayList<Integer>(lastInclusive - first + 1);
        for (int value = first; value <= lastInclusive; value++) values.add(value);
        return values;
    }

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("smoke-test delay interrupted", interrupted);
        }
    }
}
