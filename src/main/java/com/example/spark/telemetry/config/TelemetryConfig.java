package com.example.spark.telemetry.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Immutable, validated source of truth for every plugin setting.
 *
 * <p>Precedence is packaged defaults, environment, SparkConf, and finally the immutable map
 * returned by the Driver plugin to Executors.</p>
 */
public final class TelemetryConfig {
    public static final String PREFIX = "spark.telemetry.";
    public static final String ENABLED = PREFIX + "enabled";
    public static final String STRICT = PREFIX + "strict";
    public static final String ENDPOINT = PREFIX + "endpoint";
    public static final String PROFILE_ENDPOINT = PREFIX + "profile.endpoint";
    public static final String METRICS_ENABLED = PREFIX + "metrics.enabled";
    public static final String LOGS_ENABLED = PREFIX + "logs.enabled";
    public static final String TRACES_ENABLED = PREFIX + "traces.enabled";
    public static final String PROFILES_ENABLED = PREFIX + "profiles.enabled";
    public static final String LOG_MINIMUM_LEVEL = PREFIX + "logs.minimum-level";
    public static final String LOG_CAPTURE = PREFIX + "logs.capture";
    public static final String TASK_SAMPLE_RATE = PREFIX + "traces.task.sample-rate";
    public static final String SLOW_TASK_THRESHOLD = PREFIX + "traces.slow-task-threshold";
    public static final String PROFILE_SAMPLE_RATE = PREFIX + "profiles.sample-rate";
    public static final String PROFILE_WINDOW = PREFIX + "profiles.window";
    public static final String PROFILE_TRANSPORT = PREFIX + "profiles.transport";
    public static final String METRICS_QUEUE_CAPACITY = PREFIX + "queue.metrics.capacity";
    public static final String LOGS_QUEUE_CAPACITY = PREFIX + "queue.logs.capacity";
    public static final String TRACES_QUEUE_CAPACITY = PREFIX + "queue.traces.capacity";
    public static final String PROFILES_QUEUE_CAPACITY = PREFIX + "queue.profiles.capacity";
    public static final String BATCH_MAX_SIZE = PREFIX + "batch.max-size";
    public static final String BATCH_TIMEOUT = PREFIX + "batch.timeout";
    public static final String EXPORT_TIMEOUT = PREFIX + "export.timeout";
    public static final String SHUTDOWN_FLUSH_TIMEOUT = PREFIX + "shutdown.flush-timeout";
    public static final String SERVICE_NAME = PREFIX + "resource.service.name";
    public static final String SERVICE_NAMESPACE = PREFIX + "resource.service.namespace";
    public static final String DEPLOYMENT_ENVIRONMENT = PREFIX + "resource.deployment.environment";
    public static final String CLUSTER = PREFIX + "resource.cluster";
    public static final String INTERNAL_APP_NAME = PREFIX + "internal.app-name";
    public static final String INTERNAL_APP_ID = PREFIX + "internal.app-id";

    private static final List<String> KEYS = Collections.unmodifiableList(Arrays.asList(
            ENABLED, STRICT, ENDPOINT, PROFILE_ENDPOINT,
            METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED, PROFILES_ENABLED,
            LOG_MINIMUM_LEVEL, LOG_CAPTURE, TASK_SAMPLE_RATE, SLOW_TASK_THRESHOLD,
            PROFILE_SAMPLE_RATE, PROFILE_WINDOW, PROFILE_TRANSPORT,
            METRICS_QUEUE_CAPACITY, LOGS_QUEUE_CAPACITY, TRACES_QUEUE_CAPACITY,
            PROFILES_QUEUE_CAPACITY, BATCH_MAX_SIZE, BATCH_TIMEOUT, EXPORT_TIMEOUT,
            SHUTDOWN_FLUSH_TIMEOUT, SERVICE_NAME, SERVICE_NAMESPACE,
            DEPLOYMENT_ENVIRONMENT, CLUSTER, INTERNAL_APP_NAME, INTERNAL_APP_ID));

    private final Map<String, String> values;

    private TelemetryConfig(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<String, String>(values));
    }

    public static TelemetryConfig from(Map<String, String> sparkConfiguration) {
        return from(sparkConfiguration, System.getenv());
    }

    /** Safe fallback used when configuration or a provided dependency cannot be read at all. */
    public static TelemetryConfig disabled() {
        Map<String, String> disabled = defaults();
        disabled.put(ENABLED, "false");
        return new TelemetryConfig(disabled);
    }

    public static TelemetryConfig from(
            Map<String, String> sparkConfiguration,
            Map<String, String> environment) {
        Map<String, String> merged = defaults();
        merged.putAll(EnvironmentResolver.resolve(KEYS, environment));
        copyKnown(sparkConfiguration, merged);
        validateAndFailOpen(merged);
        return new TelemetryConfig(merged);
    }

    /** Rebuilds exactly the Driver-validated configuration on an Executor. */
    public static TelemetryConfig fromDriver(Map<String, String> driverConfiguration) {
        Map<String, String> merged = defaults();
        copyKnown(driverConfiguration, merged);
        validateAndFailOpen(merged);
        return new TelemetryConfig(merged);
    }

    public TelemetryConfig withApplication(String appName, String appId) {
        Map<String, String> enriched = new LinkedHashMap<String, String>(values);
        enriched.put(INTERNAL_APP_NAME, emptyTo(appName, "spark"));
        enriched.put(INTERNAL_APP_ID, emptyTo(appId, "unknown"));
        if ("spark".equals(enriched.get(SERVICE_NAME))) {
            enriched.put(SERVICE_NAME, emptyTo(appName, "spark"));
        }
        return new TelemetryConfig(enriched);
    }

    public Map<String, String> toExecutorConfiguration() {
        return values;
    }

    public boolean enabled() { return bool(ENABLED); }
    public boolean strict() { return bool(STRICT); }
    public boolean metricsEnabled() { return enabled() && bool(METRICS_ENABLED); }
    public boolean logsEnabled() { return enabled() && bool(LOGS_ENABLED); }
    public boolean tracesEnabled() { return enabled() && bool(TRACES_ENABLED); }
    public boolean profilesEnabled() { return enabled() && bool(PROFILES_ENABLED); }
    public boolean logCaptureEnabled() { return logsEnabled() && bool(LOG_CAPTURE); }
    public String endpoint() { return values.get(ENDPOINT); }
    public String profileEndpoint() { return values.get(PROFILE_ENDPOINT); }
    public LogLevel minimumLogLevel() { return LogLevel.valueOf(values.get(LOG_MINIMUM_LEVEL)); }
    public double taskSampleRate() { return number(TASK_SAMPLE_RATE); }
    public Duration slowTaskThreshold() { return duration(SLOW_TASK_THRESHOLD); }
    public int profileSampleRate() { return integer(PROFILE_SAMPLE_RATE); }
    public Duration profileWindow() { return duration(PROFILE_WINDOW); }
    public String profileTransport() { return values.get(PROFILE_TRANSPORT); }
    public int metricsQueueCapacity() { return integer(METRICS_QUEUE_CAPACITY); }
    public int logsQueueCapacity() { return integer(LOGS_QUEUE_CAPACITY); }
    public int tracesQueueCapacity() { return integer(TRACES_QUEUE_CAPACITY); }
    public int profilesQueueCapacity() { return integer(PROFILES_QUEUE_CAPACITY); }
    public int batchMaxSize() { return integer(BATCH_MAX_SIZE); }
    public Duration batchTimeout() { return duration(BATCH_TIMEOUT); }
    public Duration exportTimeout() { return duration(EXPORT_TIMEOUT); }
    public Duration shutdownFlushTimeout() { return duration(SHUTDOWN_FLUSH_TIMEOUT); }
    public String serviceName() { return values.get(SERVICE_NAME); }
    public String serviceNamespace() { return values.get(SERVICE_NAMESPACE); }
    public String deploymentEnvironment() { return values.get(DEPLOYMENT_ENVIRONMENT); }
    public String cluster() { return values.get(CLUSTER); }
    public String applicationName() { return values.get(INTERNAL_APP_NAME); }
    public String applicationId() { return values.get(INTERNAL_APP_ID); }

    public String otlpSignalEndpoint(String signal) {
        String base = trimTrailingSlash(endpoint());
        String[] standardSignals = {"metrics", "logs", "traces"};
        for (String standardSignal : standardSignals) {
            String suffix = "/v1/" + standardSignal;
            if (base.endsWith(suffix)) {
                base = base.substring(0, base.length() - suffix.length());
                break;
            }
        }
        return trimTrailingSlash(base) + "/v1/" + signal;
    }

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<String, String>();
        defaults.put(ENABLED, "true");
        defaults.put(STRICT, "false");
        defaults.put(ENDPOINT, "http://127.0.0.1:4318");
        defaults.put(PROFILE_ENDPOINT, "http://127.0.0.1:9999");
        defaults.put(METRICS_ENABLED, "true");
        defaults.put(LOGS_ENABLED, "true");
        defaults.put(TRACES_ENABLED, "true");
        defaults.put(PROFILES_ENABLED, "false");
        defaults.put(LOG_MINIMUM_LEVEL, "INFO");
        defaults.put(LOG_CAPTURE, "true");
        defaults.put(TASK_SAMPLE_RATE, "0.01");
        defaults.put(SLOW_TASK_THRESHOLD, "30s");
        defaults.put(PROFILE_SAMPLE_RATE, "19");
        defaults.put(PROFILE_WINDOW, "10s");
        defaults.put(PROFILE_TRANSPORT, "pyroscope");
        defaults.put(METRICS_QUEUE_CAPACITY, "1000");
        defaults.put(LOGS_QUEUE_CAPACITY, "10000");
        defaults.put(TRACES_QUEUE_CAPACITY, "5000");
        defaults.put(PROFILES_QUEUE_CAPACITY, "10");
        defaults.put(BATCH_MAX_SIZE, "512");
        defaults.put(BATCH_TIMEOUT, "2s");
        defaults.put(EXPORT_TIMEOUT, "10s");
        defaults.put(SHUTDOWN_FLUSH_TIMEOUT, "3s");
        defaults.put(SERVICE_NAME, "spark");
        defaults.put(SERVICE_NAMESPACE, "");
        defaults.put(DEPLOYMENT_ENVIRONMENT, "");
        defaults.put(CLUSTER, "");
        defaults.put(INTERNAL_APP_NAME, "spark");
        defaults.put(INTERNAL_APP_ID, "unknown");
        return defaults;
    }

    private static void copyKnown(Map<String, String> source, Map<String, String> target) {
        for (String key : KEYS) {
            String value = source.get(key);
            if (value != null) {
                target.put(key, value.trim());
            }
        }
    }

    private static void validateAndFailOpen(Map<String, String> values) {
        Map<String, String> packagedDefaults = defaults();
        final boolean strict;
        try {
            strict = parseBoolean(STRICT, values.get(STRICT));
            parseBoolean(ENABLED, values.get(ENABLED));
            ConfigValidator.positive(BATCH_MAX_SIZE, parseInt(BATCH_MAX_SIZE, values));
            parseDuration(BATCH_TIMEOUT, values.get(BATCH_TIMEOUT));
            parseDuration(EXPORT_TIMEOUT, values.get(EXPORT_TIMEOUT));
            parseDuration(SHUTDOWN_FLUSH_TIMEOUT, values.get(SHUTDOWN_FLUSH_TIMEOUT));
        } catch (RuntimeException invalid) {
            if ("true".equalsIgnoreCase(values.get(STRICT))) throw invalid;
            values.put(ENABLED, "false");
            reset(values, packagedDefaults, STRICT, BATCH_MAX_SIZE, BATCH_TIMEOUT,
                    EXPORT_TIMEOUT, SHUTDOWN_FLUSH_TIMEOUT);
            return;
        }

        validateSignal(values, packagedDefaults, strict, METRICS_ENABLED, new Validation() {
            @Override public void validate() {
                parseBoolean(METRICS_ENABLED, values.get(METRICS_ENABLED));
                ConfigValidator.positive(METRICS_QUEUE_CAPACITY, parseInt(METRICS_QUEUE_CAPACITY, values));
            }
        }, METRICS_QUEUE_CAPACITY);
        validateSignal(values, packagedDefaults, strict, LOGS_ENABLED, new Validation() {
            @Override public void validate() {
                parseBoolean(LOGS_ENABLED, values.get(LOGS_ENABLED));
                parseBoolean(LOG_CAPTURE, values.get(LOG_CAPTURE));
                ConfigValidator.positive(LOGS_QUEUE_CAPACITY, parseInt(LOGS_QUEUE_CAPACITY, values));
                values.put(LOG_MINIMUM_LEVEL,
                        LogLevel.valueOf(values.get(LOG_MINIMUM_LEVEL).toUpperCase(Locale.ROOT)).name());
            }
        }, LOG_CAPTURE, LOGS_QUEUE_CAPACITY, LOG_MINIMUM_LEVEL);
        validateSignal(values, packagedDefaults, strict, TRACES_ENABLED, new Validation() {
            @Override public void validate() {
                parseBoolean(TRACES_ENABLED, values.get(TRACES_ENABLED));
                ConfigValidator.positive(TRACES_QUEUE_CAPACITY, parseInt(TRACES_QUEUE_CAPACITY, values));
                ConfigValidator.rate(TASK_SAMPLE_RATE, parseDouble(TASK_SAMPLE_RATE, values));
                parseDuration(SLOW_TASK_THRESHOLD, values.get(SLOW_TASK_THRESHOLD));
            }
        }, TRACES_QUEUE_CAPACITY, TASK_SAMPLE_RATE, SLOW_TASK_THRESHOLD);
        validateSignal(values, packagedDefaults, strict, PROFILES_ENABLED, new Validation() {
            @Override public void validate() {
                parseBoolean(PROFILES_ENABLED, values.get(PROFILES_ENABLED));
                ConfigValidator.positive(PROFILES_QUEUE_CAPACITY, parseInt(PROFILES_QUEUE_CAPACITY, values));
                ConfigValidator.positive(PROFILE_SAMPLE_RATE, parseInt(PROFILE_SAMPLE_RATE, values));
                parseDuration(PROFILE_WINDOW, values.get(PROFILE_WINDOW));
                if (!"pyroscope".equalsIgnoreCase(values.get(PROFILE_TRANSPORT))) {
                    throw new IllegalArgumentException(PROFILE_TRANSPORT + " currently supports only pyroscope");
                }
            }
        }, PROFILES_QUEUE_CAPACITY, PROFILE_SAMPLE_RATE, PROFILE_WINDOW, PROFILE_TRANSPORT);

        if (!ConfigValidator.isHttpEndpoint(values.get(ENDPOINT))) {
            invalidSignal(values, strict, "OTLP endpoint is not an http(s) URI", METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED);
            if (!strict) values.put(ENDPOINT, packagedDefaults.get(ENDPOINT));
        }
        if (!ConfigValidator.isHttpEndpoint(values.get(PROFILE_ENDPOINT))) {
            invalidSignal(values, strict, "profile endpoint is not an http(s) URI", PROFILES_ENABLED);
            if (!strict) values.put(PROFILE_ENDPOINT, packagedDefaults.get(PROFILE_ENDPOINT));
        }
    }

    private static void validateSignal(
            Map<String, String> values,
            Map<String, String> packagedDefaults,
            boolean strict,
            String enabledKey,
            Validation validation,
            String... resetKeys) {
        try {
            validation.validate();
        } catch (RuntimeException invalid) {
            if (strict) throw invalid;
            values.put(enabledKey, "false");
            reset(values, packagedDefaults, resetKeys);
        }
    }

    private static void reset(
            Map<String, String> values,
            Map<String, String> packagedDefaults,
            String... keys) {
        for (String key : keys) values.put(key, packagedDefaults.get(key));
    }

    private interface Validation { void validate(); }

    private static void invalidSignal(
            Map<String, String> values,
            boolean strict,
            String message,
            String... affectedKeys) {
        if (strict) {
            throw new IllegalArgumentException(message);
        }
        for (String affectedKey : affectedKeys) {
            values.put(affectedKey, "false");
        }
    }

    private boolean bool(String key) { return parseBoolean(key, values.get(key)); }
    private int integer(String key) { return parseInt(key, values); }
    private double number(String key) { return parseDouble(key, values); }
    private Duration duration(String key) { return parseDuration(key, values.get(key)); }

    private static boolean parseBoolean(String key, String value) {
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(key + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }

    private static int parseInt(String key, Map<String, String> values) {
        try {
            return Integer.parseInt(values.get(key));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be an integer", invalid);
        }
    }

    private static double parseDouble(String key, Map<String, String> values) {
        try {
            return Double.parseDouble(values.get(key));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " must be a number", invalid);
        }
    }

    static Duration parseDuration(String key, String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        int unitStart = normalized.length();
        while (unitStart > 0 && Character.isLetter(normalized.charAt(unitStart - 1))) {
            unitStart--;
        }
        if (unitStart == 0 || unitStart == normalized.length()) {
            throw new IllegalArgumentException(key + " must include a duration unit (ms, s, m, h)");
        }
        long amount;
        try {
            amount = Long.parseLong(normalized.substring(0, unitStart));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " has an invalid duration", invalid);
        }
        if (amount < 0) {
            throw new IllegalArgumentException(key + " must not be negative");
        }
        String unit = normalized.substring(unitStart);
        if ("ms".equals(unit)) return Duration.ofMillis(amount);
        if ("s".equals(unit)) return Duration.ofSeconds(amount);
        if ("m".equals(unit)) return Duration.ofMinutes(amount);
        if ("h".equals(unit)) return Duration.ofHours(amount);
        throw new IllegalArgumentException(key + " uses an unsupported duration unit: " + unit);
    }

    private static String trimTrailingSlash(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') end--;
        return value.substring(0, end);
    }

    private static String emptyTo(String value, String fallback) {
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    public enum LogLevel {
        TRACE(0), DEBUG(1), INFO(2), WARN(3), ERROR(4), FATAL(5);

        private final int priority;

        LogLevel(int priority) { this.priority = priority; }
        public int priority() { return priority; }
        public boolean includes(LogLevel actual) { return actual.priority >= priority; }
    }
}
