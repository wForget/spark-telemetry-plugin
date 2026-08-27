package com.example.spark.telemetry.runtime;

import com.example.spark.telemetry.config.TelemetryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class DeferredTelemetrySinkTest {
    @Test
    void drainsBootstrapEventsAndClosesIdempotently() {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.METRICS_ENABLED, "false");
        values.put(TelemetryConfig.LOGS_ENABLED, "false");
        values.put(TelemetryConfig.TRACES_ENABLED, "false");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("test", "app-1");
        TelemetryRuntime runtime = TelemetryRuntime.create(config, ResourceIdentity.driver(config, "app-1"));
        DeferredTelemetrySink sink = new DeferredTelemetrySink(2);
        sink.jobStarted(1, new int[] {2}, 10L);
        sink.jobEnded(1, 10L, 20L, "success", "");
        sink.bind(runtime);

        assertDoesNotThrow(sink::close);
        assertDoesNotThrow(sink::close);
        assertDoesNotThrow(() -> runtime.close(Duration.ofMillis(10)));
    }
}
