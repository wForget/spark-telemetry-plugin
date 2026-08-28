package com.example.spark.telemetry.runtime;

import org.apache.spark.telemetry.config.TelemetryConfig;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class ResourceIdentityTest {
    @Test
    void metricProjectionExcludesHighCardinalityIdentity() {
        TelemetryConfig config = TelemetryConfig.from(
                new HashMap<String, String>(), new HashMap<String, String>())
                .withApplication("orders", "application-1");
        ResourceIdentity identity = ResourceIdentity.executor(config, "application-1", "12");
        Resource metric = identity.metricResource();
        Resource detailed = identity.detailedResource();

        assertNull(metric.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("spark.app.id")));
        assertNull(metric.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("spark.executor.id")));
        String metricInstance = metric.getAttribute(
                io.opentelemetry.api.common.AttributeKey.stringKey("service.instance.id"));
        assertNotNull(metricInstance);
        assertFalse(metricInstance.contains("application-1"));
        assertFalse(metricInstance.endsWith("/12"));
        assertEquals("executor", metric.getAttribute(
                io.opentelemetry.api.common.AttributeKey.stringKey("spark.role")));
        assertEquals("application-1/12", detailed.getAttribute(
                io.opentelemetry.api.common.AttributeKey.stringKey("service.instance.id")));
    }
}
