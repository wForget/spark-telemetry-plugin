package cn.wangz.spark.telemetry.runtime;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.resources.Resource;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.telemetry.config.TelemetryConfig;

/** Signal-aware resource projection. Short-lived identifiers never enter metric resources. */
public final class ResourceIdentity {
    private static final String PROCESS_INSTANCE_ID = processInstanceId();
    private final String serviceName;
    private final String serviceNamespace;
    private final String deploymentEnvironment;
    private final String cluster;
    private final String applicationName;
    private final String applicationId;
    private final String role;
    private final String executorId;
    private final boolean localMode;

    private ResourceIdentity(
            String serviceName,
            String serviceNamespace,
            String deploymentEnvironment,
            String cluster,
            String applicationName,
            String applicationId,
            String role,
            String executorId,
            boolean localMode) {
        this.serviceName = serviceName;
        this.serviceNamespace = serviceNamespace;
        this.deploymentEnvironment = deploymentEnvironment;
        this.cluster = cluster;
        this.applicationName = applicationName;
        this.applicationId = applicationId;
        this.role = role;
        this.executorId = executorId;
        this.localMode = localMode;
    }

    public static ResourceIdentity driver(TelemetryConfig config, String applicationId) {
        return driver(config, applicationId, false);
    }

    public static ResourceIdentity driver(
            TelemetryConfig config, String applicationId, boolean localMode) {
        return create(config, applicationId, "driver", "driver", localMode);
    }

    public static ResourceIdentity executor(TelemetryConfig config, String applicationId, String executorId) {
        return create(config, applicationId, "executor", executorId, false);
    }

    private static ResourceIdentity create(
            TelemetryConfig config,
            String applicationId,
            String role,
            String executorId,
            boolean localMode) {
        return new ResourceIdentity(
                config.serviceName(), config.serviceNamespace(), config.deploymentEnvironment(), config.cluster(),
                config.applicationName(), valueOr(applicationId, config.applicationId()), role,
                valueOr(executorId, "unknown"), localMode);
    }

    public Resource metricResource() {
        AttributesBuilder attributes = stableAttributes();
        // Cumulative OTLP streams need a writer identity. This opaque JVM identity avoids
        // collisions without exposing Spark app/executor/job/stage/task identifiers.
        attributes.put("service.instance.id", PROCESS_INSTANCE_ID);
        attributes.put("spark.role", role);
        return Resource.create(attributes.build());
    }

    /** Stable metric dimension promoted by Prometheus exporters to {@code spark_executor_id}. */
    public Attributes metricAttributes() {
        return Attributes.builder().put("spark.executor.id", executorId).build();
    }

    public Resource detailedResource() {
        AttributesBuilder attributes = stableAttributes();
        attributes.put("service.instance.id", applicationId + "/" + executorId);
        attributes.put("spark.app.name", applicationName);
        attributes.put("spark.app.id", applicationId);
        attributes.put("spark.role", role);
        if ("executor".equals(role)) {
            attributes.put("spark.executor.id", executorId);
        }
        return Resource.create(attributes.build());
    }

    /** Static Pyroscope labels. Job, stage and task identifiers are intentionally excluded. */
    public Map<String, String> profileLabels() {
        Map<String, String> labels = new LinkedHashMap<String, String>();
        putIfPresent(labels, "service_namespace", serviceNamespace);
        putIfPresent(labels, "deployment_environment", deploymentEnvironment);
        putIfPresent(labels, "spark_cluster", cluster);
        putIfPresent(labels, "spark_app_name", applicationName);
        putIfPresent(labels, "spark_app_id", applicationId);
        labels.put("spark_role", localMode ? "local_jvm" : role);
        if ("executor".equals(role)) labels.put("spark_executor_id", executorId);
        labels.put("service_instance_id", applicationId + "/" + executorId);
        if (localMode) labels.put("spark_local_mode", "true");
        return Collections.unmodifiableMap(labels);
    }

    private AttributesBuilder stableAttributes() {
        AttributesBuilder attributes = Attributes.builder().put("service.name", serviceName);
        putIfPresent(attributes, "service.namespace", serviceNamespace);
        putIfPresent(attributes, "deployment.environment.name", deploymentEnvironment);
        putIfPresent(attributes, "spark.cluster", cluster);
        return attributes;
    }

    private static void putIfPresent(AttributesBuilder attributes, String key, String value) {
        if (value != null && !value.isEmpty()) attributes.put(key, value);
    }

    private static void putIfPresent(Map<String, String> labels, String key, String value) {
        if (value != null && !value.isEmpty()) labels.put(key, value);
    }

    private static String valueOr(String value, String fallback) {
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static String processInstanceId() {
        try {
            java.lang.management.RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String source = runtime.getName() + "/" + runtime.getStartTime();
            return "jvm-" + UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException unavailable) {
            return "jvm-" + Long.toHexString(System.currentTimeMillis());
        }
    }
}
