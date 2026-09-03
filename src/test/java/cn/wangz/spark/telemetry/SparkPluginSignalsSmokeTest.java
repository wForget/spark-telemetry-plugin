package cn.wangz.spark.telemetry;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricRegistry;
import io.pyroscope.labels.pb.JfrLabels;
import io.pyroscope.labels.v2.Pyroscope;
import io.pyroscope.javaagent.PyroscopeAgent;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkPluginSignalsSmokeTest {
    private static final Logger LOGGER = LogManager.getLogger("spark.smoke.telemetry");
    private static final int LARGE_OBJECT_BYTES = 4 * 1024 * 1024;
    private static final int LARGE_OBJECT_COUNT = 8;
    private static final long PROFILER_START_TIMEOUT_SECONDS = 5L;
    private static final long PROFILER_STOP_TIMEOUT_SECONDS = 15L;
    private static final AtomicInteger INTENTIONAL_FAILURES = new AtomicInteger();
    private static final AtomicInteger RETRIED_SUCCESSES = new AtomicInteger();

    @Test
    void coversSuccessFailureSkewShuffleMemoryAndProfileSignals() {
        INTENTIONAL_FAILURES.set(0);
        RETRIED_SUCCESSES.set(0);

        // SparkConf loads spark.* JVM properties. To use a remote collector, run this test with
        // -Dspark.telemetry.endpoint=http://<alloy-host>:4317 and
        // -Dspark.telemetry.profile.endpoint=http://<alloy-host>:9999.
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
                .set("spark.telemetry.traces.slow-task-threshold", "300ms")
                .set("spark.telemetry.profiles.enabled", "true")
                .set("spark.telemetry.profiles.stage-labels.enabled", "true")
                .set("spark.telemetry.profiles.event",
                        System.getProperty("spark.telemetry.profiles.event", "wall"))
                .set("spark.telemetry.profiles.interval", "20ms")
                .set("spark.telemetry.profiles.upload-interval", "2s")
                .set("spark.telemetry.profiles.async-profiler.extra-arguments",
                        "cstack=vmx,memlimit=32m");

        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            assertTrue(await(PyroscopeAgent::isStarted, PROFILER_START_TIMEOUT_SECONDS),
                    "Pyroscope profiler should start with the Spark plugin");
            LOGGER.warn("Starting all-signal Spark telemetry smoke scenarios");
            assertStageProfileLabels(context);
            runSuccessfulTransformations(context);
            runFailureAndRetry(context);
            runSkewedTasks(context);
            runSkewedShuffle(context);
            runLargeObjectMemoryPressure(context);
            // Keep the application alive for at least one Pyroscope upload interval.
            sleepMillis(3000L);
            LOGGER.warn("Completed all-signal Spark telemetry smoke scenarios");
        } finally {
            context.stop();
        }
        assertTrue(await(() -> !PyroscopeAgent.isStarted(), PROFILER_STOP_TIMEOUT_SECONDS),
                "Pyroscope profiler should stop with the Spark plugin");
    }

    private static void assertStageProfileLabels(JavaSparkContext context) {
        LOGGER.warn("Scenario: stage-scoped profile labels");
        List<Boolean> matches = context.parallelize(Arrays.asList(1), 1)
                .map(ignored -> currentStageProfileLabelsPresent())
                .collect();

        assertEquals(Arrays.asList(Boolean.TRUE), matches,
                "the task thread should expose its current Spark stage to Pyroscope");
    }

    private static boolean currentStageProfileLabelsPresent() {
        TaskContext task = TaskContext.get();
        String expectedStageId = String.valueOf(task.stageId());
        String expectedStageAttempt = String.valueOf(task.stageAttemptNumber());
        JfrLabels.LabelsSnapshot snapshot = Pyroscope.LabelsWrapper.dump();
        Map<Long, String> strings = snapshot.getStringsMap();
        for (JfrLabels.Context profileContext : snapshot.getContextsMap().values()) {
            boolean stageIdMatches = false;
            boolean stageAttemptMatches = false;
            for (Map.Entry<Long, Long> label : profileContext.getLabelsMap().entrySet()) {
                String key = strings.get(label.getKey());
                String value = strings.get(label.getValue());
                if ("spark_stage_id".equals(key) && expectedStageId.equals(value)) {
                    stageIdMatches = true;
                } else if ("spark_stage_attempt".equals(key)
                        && expectedStageAttempt.equals(value)) {
                    stageAttemptMatches = true;
                }
            }
            if (stageIdMatches && stageAttemptMatches) return true;
        }
        return false;
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

    private static boolean await(BooleanSupplier condition, long timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return true;
            sleepMillis(25L);
        }
        return condition.getAsBoolean();
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
