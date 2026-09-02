package cn.wangz.spark.telemetry;

import io.pyroscope.javaagent.PyroscopeAgent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in process test because it loads the native async-profiler library into the test JVM. */
class SparkPluginProfileSmokeTest {
    @Test
    void startsOneLocalJvmProfilerFromPluginCode() {
        Assumptions.assumeTrue(Boolean.getBoolean("spark.telemetry.profile.smoke"),
                "enable with -Dspark.telemetry.profile.smoke=true");

        SparkConf conf = new SparkConf(false)
                .setMaster("local[2]")
                .setAppName("telemetry-plugin-profile-smoke")
                .set("spark.ui.enabled", "false")
                .set("spark.plugins", SparkTelemetryPlugin.class.getName())
                .set("spark.telemetry.strict", "true")
                .set("spark.telemetry.metrics.enabled", "false")
                .set("spark.telemetry.logs.enabled", "false")
                .set("spark.telemetry.traces.enabled", "false")
                .set("spark.telemetry.profiles.enabled", "true")
                .set("spark.telemetry.profile.endpoint", "http://127.0.0.1:1")
                .set("spark.telemetry.profiles.event", "wall")
                .set("spark.telemetry.profiles.interval", "20ms")
                .set("spark.telemetry.profiles.upload-interval", "2s")
                .set("spark.telemetry.profiles.async-profiler.extra-arguments",
                        "cstack=vmx,memlimit=32m")
                .set("spark.telemetry.shutdown.flush-timeout", "3s");

        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            assertTrue(await(PyroscopeAgent::isStarted, DurationSeconds.FIVE));
            assertEquals(4L, context.parallelize(Arrays.asList(1, 2, 3, 4), 2)
                    .map(value -> value * value)
                    .count());
            sleepMillis(3000L);
        } finally {
            context.stop();
        }
        assertTrue(await(() -> !PyroscopeAgent.isStarted(), DurationSeconds.FIFTEEN));
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
            throw new IllegalStateException("profile smoke wait interrupted", interrupted);
        }
    }

    private static final class DurationSeconds {
        private static final long FIVE = 5L;
        private static final long FIFTEEN = 15L;
    }
}
