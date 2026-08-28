package com.example.spark.telemetry.config;

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
                .set(TelemetryConfig.SHUTDOWN_FLUSH_TIMEOUT().key(), "2500ms");

        TelemetryConfig driver = TelemetryConfig.from(spark, new HashMap<String, String>());
        TelemetryConfig executor = TelemetryConfig.fromDriver(driver.toExecutorConfiguration());

        assertEquals(TelemetryLogLevel.WARN, executor.minimumLogLevel());
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
}
