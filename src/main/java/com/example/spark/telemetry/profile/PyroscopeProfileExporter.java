package com.example.spark.telemetry.profile;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** Pyroscope HTTP push transport targeting Alloy's pyroscope.receive_http endpoint. */
public final class PyroscopeProfileExporter implements ProfileExporter {
    private final URI ingestEndpoint;
    private final int timeoutMillis;

    public PyroscopeProfileExporter(String endpoint, Duration timeout) {
        String base = trimSlash(endpoint);
        this.ingestEndpoint = URI.create(base.endsWith("/ingest") ? base : base + "/ingest");
        this.timeoutMillis = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, timeout.toMillis()));
    }

    @Override
    public ExportResult export(ProfileBatch batch) {
        HttpURLConnection connection = null;
        try {
            URI request = URI.create(ingestEndpoint.toString() + query(batch));
            connection = (HttpURLConnection) request.toURL().openConnection();
            connection.setConnectTimeout(timeoutMillis);
            connection.setReadTimeout(timeoutMillis);
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", batch.contentType());
            byte[] payload = batch.payload();
            connection.setFixedLengthStreamingMode(payload.length);
            connection.getOutputStream().write(payload);
            int status = connection.getResponseCode();
            drain(status >= 400 ? connection.getErrorStream() : connection.getInputStream());
            if (status >= 200 && status < 300) return ExportResult.success(status);
            return ExportResult.failure(status == 408 || status == 429 || status >= 500, status, "HTTP " + status);
        } catch (IOException failure) {
            return ExportResult.failure(true, -1, failure.getClass().getSimpleName());
        } catch (RuntimeException failure) {
            return ExportResult.failure(false, -1, failure.getClass().getSimpleName());
        } finally {
            if (connection != null) connection.disconnect();
        }
    }

    @Override
    public void close() {
        // HttpURLConnection has no process-wide client to close.
    }

    private static String query(ProfileBatch batch) {
        StringBuilder series = new StringBuilder(batch.serviceName());
        if (!batch.labels().isEmpty()) {
            series.append('{');
            boolean first = true;
            for (Map.Entry<String, String> label : batch.labels().entrySet()) {
                if (!first) series.append(',');
                series.append(label.getKey()).append('=').append(label.getValue());
                first = false;
            }
            series.append('}');
        }
        return "?name=" + encode(series.toString())
                + "&from=" + TimeUnitSeconds.fromMillis(batch.fromEpochMillis())
                + "&until=" + TimeUnitSeconds.fromMillis(batch.untilEpochMillis())
                + "&format=" + encode(batch.format())
                + "&sampleRate=" + batch.sampleRate();
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }
    private static void drain(InputStream stream) throws IOException {
        if (stream == null) return;
        try {
            byte[] buffer = new byte[512];
            while (stream.read(buffer) >= 0) { /* discard bounded response chunks */ }
        } finally {
            stream.close();
        }
    }
    private static String trimSlash(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') end--;
        return value.substring(0, end);
    }

    private static final class TimeUnitSeconds {
        static long fromMillis(long millis) { return millis / 1000L; }
    }
}
