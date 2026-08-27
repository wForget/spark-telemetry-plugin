package com.example.spark.telemetry.config;

import java.util.LinkedHashMap;
import java.util.Map;

/** Maps Spark-style configuration keys to conventional upper-case environment names. */
public final class EnvironmentResolver {
    private EnvironmentResolver() {
    }

    public static Map<String, String> resolve(Iterable<String> keys, Map<String, String> environment) {
        Map<String, String> resolved = new LinkedHashMap<String, String>();
        for (String key : keys) {
            String value = environment.get(toEnvironmentName(key));
            if (value != null && !value.trim().isEmpty()) {
                resolved.put(key, value.trim());
            }
        }
        return resolved;
    }

    public static String toEnvironmentName(String sparkKey) {
        return sparkKey.toUpperCase().replace('.', '_').replace('-', '_');
    }
}
