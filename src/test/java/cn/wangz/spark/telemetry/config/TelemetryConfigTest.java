package cn.wangz.spark.telemetry.config;

import org.junit.jupiter.api.Test;
import org.apache.spark.telemetry.config.TelemetryConfig;
import org.apache.spark.telemetry.config.TelemetryLogLevel;
import org.apache.spark.SparkConf;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TelemetryConfigTest {
    @Test
    void defaultsMinimumLogLevelToError() {
        TelemetryConfig config = TelemetryConfig.from(
                new HashMap<String, String>(), new HashMap<String, String>());

        assertEquals(TelemetryLogLevel.ERROR, config.minimumLogLevel());
        assertFalse(config.profilesEnabled());
        assertEquals("http://127.0.0.1:9999", config.profileEndpoint());
        assertEquals("ITIMER", config.profileEvent());
        assertEquals(Duration.ofMillis(10), config.profileInterval());
        assertEquals(Duration.ofSeconds(10), config.profileUploadInterval());
        assertEquals("memlimit=128m", config.asyncProfilerExtraArguments());
    }

    @Test
    void appliesDefaultsEnvironmentAndSparkPrecedence() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("SPARK_TELEMETRY_TRACES_TASK_SAMPLE_RATE", "0.25");
        environment.put("SPARK_TELEMETRY_LOGS_MINIMUM_LEVEL", "WARN");
        Map<String, String> spark = new HashMap<String, String>();
        spark.put(TelemetryConfig.TASK_SAMPLE_RATE().key(), "0.5");

        TelemetryConfig config = TelemetryConfig.from(spark, environment);

        assertEquals(0.5d, config.taskSampleRate());
        assertEquals(TelemetryLogLevel.WARN, config.minimumLogLevel());
        assertEquals(Duration.ofSeconds(30), config.slowTaskThreshold());
        assertEquals("http://127.0.0.1:4317", config.endpoint());
    }

    @Test
    void driverMapIsAllowlistedAndImmutable() {
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("spark.telemetry.authorization", "secret");
        spark.put(TelemetryConfig.ENDPOINT().key(), "http://alloy:4317/");
        TelemetryConfig config = TelemetryConfig.from(spark, new HashMap<String, String>());

        assertFalse(config.toExecutorConfiguration().containsKey("spark.telemetry.authorization"));
        assertEquals("http://alloy:4317/", config.endpoint());
        assertThrows(UnsupportedOperationException.class,
                () -> config.toExecutorConfiguration().put(TelemetryConfig.ENABLED().key(), "false"));
    }

    @Test
    void invalidConfigurationFailsOpenUnlessStrict() {
        Map<String, String> failOpen = new HashMap<String, String>();
        failOpen.put(TelemetryConfig.BATCH_MAX_SIZE().key(), "0");
        assertFalse(TelemetryConfig.from(failOpen, new HashMap<String, String>()).enabled());

        Map<String, String> strict = new HashMap<String, String>();
        strict.put(TelemetryConfig.STRICT().key(), "true");
        strict.put(TelemetryConfig.TASK_SAMPLE_RATE().key(), "NaN");
        assertThrows(IllegalArgumentException.class,
                () -> TelemetryConfig.from(strict, new HashMap<String, String>()));
    }

    @Test
    void invalidSignalOnlyDisablesThatSignal() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.TASK_SAMPLE_RATE().key(), "2.0");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertTrue(config.enabled());
        assertTrue(config.metricsEnabled());
        assertFalse(config.tracesEnabled());
        assertEquals(0.01d, config.taskSampleRate());
    }

    @Test
    void unsafeEndpointPartsAreNeverPropagated() {
        String[] unsafeEndpoints = {
                "http://user:secret@alloy:4317",
                "https://alloy:4317?token=secret",
                "https://alloy:4317/#secret"
        };
        for (String endpoint : unsafeEndpoints) {
            Map<String, String> values = new HashMap<String, String>();
            values.put(TelemetryConfig.ENDPOINT().key(), endpoint);
            TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

            assertFalse(config.metricsEnabled());
            assertFalse(config.logsEnabled());
            assertFalse(config.tracesEnabled());
            assertEquals("http://127.0.0.1:4317", config.endpoint());
            assertFalse(config.toExecutorConfiguration().toString().contains("secret"));
        }
    }

    @Test
    void grpcEndpointMustBeABaseUri() {
        String[] nonBaseEndpoints = {
                "http://alloy:4317/v1/metrics",
                "http://alloy:4317/custom",
                "HTTP://alloy:4317",
                "http://alloy:0",
                "http://alloy:65536",
                "http://alloy:99999"
        };
        for (String endpoint : nonBaseEndpoints) {
            Map<String, String> values = new HashMap<String, String>();
            values.put(TelemetryConfig.ENDPOINT().key(), endpoint);
            TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

            assertFalse(config.metricsEnabled());
            assertFalse(config.logsEnabled());
            assertFalse(config.tracesEnabled());
            assertEquals("http://127.0.0.1:4317", config.endpoint());
        }
    }

    @Test
    void readsSparkConfEntriesAndUsesSparkTimeSyntax() {
        SparkConf spark = new SparkConf(false)
                .set(TelemetryConfig.TASK_SAMPLE_RATE().key(), "0.4")
                .set(TelemetryConfig.BATCH_TIMEOUT().key(), "1500ms")
                .set(TelemetryConfig.SLOW_TASK_THRESHOLD().key(), "2min");
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("SPARK_TELEMETRY_TRACES_TASK_SAMPLE_RATE", "0.2");

        TelemetryConfig config = TelemetryConfig.from(spark, environment);

        assertEquals(0.4d, config.taskSampleRate());
        assertEquals(Duration.ofMillis(1500), config.batchTimeout());
        assertEquals(Duration.ofMinutes(2), config.slowTaskThreshold());
        assertEquals("1500ms", config.toExecutorConfiguration()
                .get(TelemetryConfig.BATCH_TIMEOUT().key()));
    }

    @Test
    void driverConfigurationRoundTripsCanonicalTypedValues() {
        SparkConf spark = new SparkConf(false)
                .set(TelemetryConfig.LOG_MINIMUM_LEVEL().key(), "warn")
                .set(TelemetryConfig.PROFILES_ENABLED().key(), "true")
                .set(TelemetryConfig.PROFILE_EVENT().key(), "wall")
                .set(TelemetryConfig.PROFILE_INTERVAL().key(), "20ms")
                .set(TelemetryConfig.PROFILE_UPLOAD_INTERVAL().key(), "15s")
                .set(TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "cstack=dwarf,memlimit=64m")
                .set(TelemetryConfig.SHUTDOWN_FLUSH_TIMEOUT().key(), "2500ms");

        TelemetryConfig driver = TelemetryConfig.from(spark, new HashMap<String, String>());
        TelemetryConfig executor = TelemetryConfig.fromDriver(driver.toExecutorConfiguration());

        assertEquals(TelemetryLogLevel.WARN, executor.minimumLogLevel());
        assertTrue(executor.profilesEnabled());
        assertEquals("WALL", executor.profileEvent());
        assertEquals(Duration.ofMillis(20), executor.profileInterval());
        assertEquals(Duration.ofSeconds(15), executor.profileUploadInterval());
        assertEquals("cstack=dwarf,memlimit=64m", executor.asyncProfilerExtraArguments());
        assertEquals(Duration.ofMillis(2500), executor.shutdownFlushTimeout());
        assertEquals(driver.toExecutorConfiguration(), executor.toExecutorConfiguration());
    }

    @Test
    void variableSubstitutionCanNeverPropagateSecrets() {
        String property = "spark.telemetry.test.secret";
        System.setProperty(property, "do-not-propagate");
        try {
            Map<String, String> spark = new HashMap<String, String>();
            spark.put(TelemetryConfig.SERVICE_NAME().key(), "${system:" + property + "}");
            TelemetryConfig driver = TelemetryConfig.from(spark, new HashMap<String, String>());

            assertFalse(driver.enabled());
            assertEquals("spark", driver.serviceName());
            assertFalse(driver.toExecutorConfiguration().toString().contains("do-not-propagate"));

            Map<String, String> executorValues = new HashMap<String, String>(
                    TelemetryConfig.from(new HashMap<String, String>(), new HashMap<String, String>())
                            .toExecutorConfiguration());
            executorValues.put(TelemetryConfig.SERVICE_NAME().key(), "${env:TELEMETRY_SECRET}");
            TelemetryConfig executor = TelemetryConfig.fromDriver(executorValues);
            assertFalse(executor.enabled());
            assertFalse(executor.toExecutorConfiguration().toString().contains("TELEMETRY_SECRET"));
        } finally {
            System.clearProperty(property);
        }
    }

    @Test
    void emptyDriverConfigurationKeepsExecutorDisabled() {
        TelemetryConfig config = TelemetryConfig.fromDriver(new HashMap<String, String>());

        assertFalse(config.enabled());
        assertFalse(config.metricsEnabled());
        assertFalse(config.logsEnabled());
        assertFalse(config.tracesEnabled());
        assertFalse(config.profilesEnabled());
    }

    @Test
    void endpointSubstitutionFailsOpenOnlyAffectedSignals() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.ENDPOINT().key(),
                "http://alloy:4317/${system:spark.telemetry.test.secret}");

        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertTrue(config.enabled());
        assertFalse(config.metricsEnabled());
        assertFalse(config.logsEnabled());
        assertFalse(config.tracesEnabled());
        assertEquals("http://127.0.0.1:4317", config.endpoint());
    }

    @Test
    void invalidProfileConfigurationOnlyDisablesProfiles() {
        String[][] invalidSettings = {
                {TelemetryConfig.PROFILE_ENDPOINT().key(), "http://alloy:9999/ingest"},
                {TelemetryConfig.PROFILE_INTERVAL().key(), "0ms"},
                {TelemetryConfig.PROFILE_INTERVAL().key(), "1ms"},
                {TelemetryConfig.PROFILE_UPLOAD_INTERVAL().key(), "0s"},
                {TelemetryConfig.PROFILE_UPLOAD_INTERVAL().key(), "1s"},
                {TelemetryConfig.PROFILE_EVENT().key(), "alloc"},
                {TelemetryConfig.PROFILE_ALLOC().key(), "0"},
                {TelemetryConfig.PROFILE_ALLOC().key(), "1"},
                {TelemetryConfig.PROFILE_LOCK().key(), "0ms"},
                {TelemetryConfig.PROFILE_LOCK().key(), "1ns"},
                {TelemetryConfig.PROFILE_JAVA_STACK_DEPTH().key(), "4097"},
                {TelemetryConfig.PROFILES_QUEUE_CAPACITY().key(), "65"},
                {TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "event=cpu"},
                {TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "e=cpu"},
                {TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "i=1000000"},
                {TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "safemode=0"},
                {TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "memlimit=2g"}
        };
        for (String[] setting : invalidSettings) {
            Map<String, String> values = new HashMap<String, String>();
            values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
            values.put(setting[0], setting[1]);

            TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

            assertTrue(config.enabled());
            assertTrue(config.metricsEnabled());
            assertTrue(config.logsEnabled());
            assertTrue(config.tracesEnabled());
            assertFalse(config.profilesEnabled(), setting[0]);
        }
    }

    @Test
    void strictInvalidProfileConfigurationFailsInitialization() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.STRICT().key(), "true");
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
        values.put(TelemetryConfig.PROFILE_INTERVAL().key(), "0ms");

        assertThrows(IllegalArgumentException.class,
                () -> TelemetryConfig.from(values, new HashMap<String, String>()));
    }

    @Test
    void profileApplicationNameCannotUsePyroscopeInlineLabelSyntax() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
        values.put(TelemetryConfig.SERVICE_NAME().key(), "orders{tenant=secret}");

        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertTrue(config.enabled());
        assertFalse(config.profilesEnabled());
        assertTrue(config.metricsEnabled());
    }

    @Test
    void inferredProfileApplicationNameIsRevalidatedAfterSparkIdentityIsKnown() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");

        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("orders{tenant=secret}", "application-1");

        assertTrue(config.enabled());
        assertFalse(config.profilesEnabled());
        assertEquals("orders{tenant=secret}", config.serviceName());
    }

    @Test
    void sparkProfileSettingsOverrideEnvironmentAndPropagate() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("SPARK_TELEMETRY_PROFILES_ENABLED", "true");
        environment.put("SPARK_TELEMETRY_PROFILES_EVENT", "wall");
        Map<String, String> spark = new HashMap<String, String>();
        spark.put(TelemetryConfig.PROFILE_EVENT().key(), "cpu");

        TelemetryConfig config = TelemetryConfig.from(spark, environment);

        assertTrue(config.profilesEnabled());
        assertEquals("CPU", config.profileEvent());
        assertEquals("CPU", config.toExecutorConfiguration().get(
                TelemetryConfig.PROFILE_EVENT().key()));
    }

    @Test
    void customCStackKeepsTheSafeDefaultMemoryLimit() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
        values.put(TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(), "cstack=dwarf");

        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertTrue(config.profilesEnabled());
        assertEquals("cstack=dwarf,memlimit=128m", config.asyncProfilerExtraArguments());
    }
}
