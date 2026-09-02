package cn.wangz.spark.telemetry.runtime;

import org.apache.spark.telemetry.config.TelemetryConfig;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ResourceIdentityTest {
    @Test
    void metricProjectionSeparatesExecutorDimensionFromWriterResource() {
        TelemetryConfig config = TelemetryConfig.from(
                new HashMap<String, String>(), new HashMap<String, String>())
                .withApplication("orders", "application-1");
        ResourceIdentity identity = ResourceIdentity.executor(config, "application-1", "12");
        Resource metric = identity.metricResource();
        Resource detailed = identity.detailedResource();

        assertNull(metric.getAttribute(io.opentelemetry.api.common.AttributeKey.stringKey("spark.app.id")));
        assertNull(metric.getAttribute(
                io.opentelemetry.api.common.AttributeKey.stringKey("spark.executor.id")));
        assertEquals("12", identity.metricAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("spark.executor.id")));
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

    @Test
    void profileProjectionUsesPyroscopeSafeStaticLabels() {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.SERVICE_NAMESPACE().key(), "data-platform");
        values.put(TelemetryConfig.DEPLOYMENT_ENVIRONMENT().key(), "production");
        values.put(TelemetryConfig.CLUSTER().key(), "compute-a");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("orders", "application-1");

        Map<String, String> labels = ResourceIdentity.executor(
                config, "application-1", "12").profileLabels();

        assertEquals("data-platform", labels.get("service_namespace"));
        assertEquals("production", labels.get("deployment_environment"));
        assertEquals("orders", labels.get("spark_app_name"));
        assertEquals("application-1", labels.get("spark_app_id"));
        assertEquals("executor", labels.get("spark_role"));
        assertEquals("12", labels.get("spark_executor_id"));
        assertEquals("application-1/12", labels.get("service_instance_id"));
        assertFalse(labels.containsKey("spark_job_id"));
        assertThrows(UnsupportedOperationException.class,
                () -> labels.put("unexpected", "value"));
    }

    @Test
    void localDriverProfileDescribesTheSharedJvm() {
        TelemetryConfig config = TelemetryConfig.from(
                new HashMap<String, String>(), new HashMap<String, String>())
                .withApplication("local-job", "local-1");

        Map<String, String> labels = ResourceIdentity.driver(
                config, "local-1", true).profileLabels();

        assertEquals("local_jvm", labels.get("spark_role"));
        assertEquals("true", labels.get("spark_local_mode"));
        assertFalse(labels.containsKey("spark_executor_id"));
    }
}
