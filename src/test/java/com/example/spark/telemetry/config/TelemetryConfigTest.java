package com.example.spark.telemetry.config;

import org.junit.jupiter.api.Test;

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
        spark.put(TelemetryConfig.TASK_SAMPLE_RATE, "0.5");

        TelemetryConfig config = TelemetryConfig.from(spark, environment);

        assertEquals(0.5d, config.taskSampleRate());
        assertEquals(TelemetryConfig.LogLevel.WARN, config.minimumLogLevel());
        assertEquals(Duration.ofSeconds(30), config.slowTaskThreshold());
    }

    @Test
    void driverMapIsAllowlistedAndImmutable() {
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("spark.telemetry.authorization", "secret");
        spark.put(TelemetryConfig.ENDPOINT, "http://alloy:4318/");
        TelemetryConfig config = TelemetryConfig.from(spark, new HashMap<String, String>());

        assertFalse(config.toExecutorConfiguration().containsKey("spark.telemetry.authorization"));
        assertEquals("http://alloy:4318/v1/traces", config.otlpSignalEndpoint("traces"));
        assertThrows(UnsupportedOperationException.class,
                () -> config.toExecutorConfiguration().put(TelemetryConfig.ENABLED, "false"));
    }

    @Test
    void invalidConfigurationFailsOpenUnlessStrict() {
        Map<String, String> failOpen = new HashMap<String, String>();
        failOpen.put(TelemetryConfig.BATCH_MAX_SIZE, "0");
        assertFalse(TelemetryConfig.from(failOpen, new HashMap<String, String>()).enabled());

        Map<String, String> strict = new HashMap<String, String>();
        strict.put(TelemetryConfig.STRICT, "true");
        strict.put(TelemetryConfig.TASK_SAMPLE_RATE, "NaN");
        assertThrows(IllegalArgumentException.class,
                () -> TelemetryConfig.from(strict, new HashMap<String, String>()));
    }

    @Test
    void invalidSignalOnlyDisablesThatSignal() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.TASK_SAMPLE_RATE, "2.0");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertTrue(config.enabled());
        assertTrue(config.metricsEnabled());
        assertFalse(config.tracesEnabled());
        assertEquals(0.01d, config.taskSampleRate());
    }

    @Test
    void endpointCredentialsAndQueryAreNeverPropagated() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.ENDPOINT, "http://user:password@alloy:4318");
        values.put(TelemetryConfig.PROFILE_ENDPOINT, "https://alloy:9999?token=secret");
        values.put(TelemetryConfig.PROFILES_ENABLED, "true");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertFalse(config.metricsEnabled());
        assertFalse(config.logsEnabled());
        assertFalse(config.tracesEnabled());
        assertFalse(config.profilesEnabled());
        assertEquals("http://127.0.0.1:4318", config.endpoint());
        assertEquals("http://127.0.0.1:9999", config.profileEndpoint());
        assertFalse(config.toExecutorConfiguration().toString().contains("secret"));
    }

    @Test
    void fullSignalEndpointIsNormalizedForEverySignal() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.ENDPOINT, "http://alloy:4318/v1/traces");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>());

        assertEquals("http://alloy:4318/v1/metrics", config.otlpSignalEndpoint("metrics"));
        assertEquals("http://alloy:4318/v1/logs", config.otlpSignalEndpoint("logs"));
        assertEquals("http://alloy:4318/v1/traces", config.otlpSignalEndpoint("traces"));
    }
}
