package com.example.spark.telemetry.config;

import java.net.URI;
import java.net.URISyntaxException;

final class ConfigValidator {
    private ConfigValidator() {
    }

    static boolean isHttpEndpoint(String value) {
        try {
            URI uri = new URI(value);
            return uri.getHost() != null
                    && uri.getUserInfo() == null
                    && uri.getQuery() == null
                    && uri.getFragment() == null
                    && ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()));
        } catch (URISyntaxException ignored) {
            return false;
        }
    }

    static int positive(String key, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be greater than zero");
        }
        return value;
    }

    static double rate(String key, double value) {
        if (Double.isNaN(value) || value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException(key + " must be in [0, 1]");
        }
        return value;
    }
}
