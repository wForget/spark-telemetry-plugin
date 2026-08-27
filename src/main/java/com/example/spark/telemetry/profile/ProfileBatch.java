package com.example.spark.telemetry.profile;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One complete, bounded profiler window. Payload is produced by an external/native collector. */
public final class ProfileBatch {
    private final String serviceName;
    private final String format;
    private final String contentType;
    private final long fromEpochMillis;
    private final long untilEpochMillis;
    private final int sampleRate;
    private final Map<String, String> labels;
    private final byte[] payload;

    public ProfileBatch(
            String serviceName,
            String format,
            String contentType,
            long fromEpochMillis,
            long untilEpochMillis,
            int sampleRate,
            Map<String, String> labels,
            byte[] payload) {
        if (serviceName == null || serviceName.isEmpty()) throw new IllegalArgumentException("serviceName is required");
        if (format == null || format.isEmpty()) throw new IllegalArgumentException("format is required");
        if (payload == null || payload.length == 0) throw new IllegalArgumentException("profile payload is required");
        if (untilEpochMillis < fromEpochMillis) throw new IllegalArgumentException("profile window is reversed");
        this.serviceName = serviceName;
        this.format = format;
        this.contentType = contentType == null ? "application/octet-stream" : contentType;
        this.fromEpochMillis = fromEpochMillis;
        this.untilEpochMillis = untilEpochMillis;
        this.sampleRate = sampleRate;
        this.labels = Collections.unmodifiableMap(new LinkedHashMap<String, String>(labels));
        this.payload = payload.clone();
    }

    public String serviceName() { return serviceName; }
    public String format() { return format; }
    public String contentType() { return contentType; }
    public long fromEpochMillis() { return fromEpochMillis; }
    public long untilEpochMillis() { return untilEpochMillis; }
    public int sampleRate() { return sampleRate; }
    public Map<String, String> labels() { return labels; }
    public byte[] payload() { return payload.clone(); }
}
