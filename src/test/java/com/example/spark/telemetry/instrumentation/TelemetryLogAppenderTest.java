package com.example.spark.telemetry.instrumentation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryLogAppenderTest {
    @Test
    void boundsAStackTraceContainingAnOversizedSingleLineMessage() {
        StringBuilder message = new StringBuilder(1_000_000);
        for (int index = 0; index < 1_000_000; index++) message.append('x');

        String stack = TelemetryLogAppender.stackTrace(new IllegalStateException(message.toString()));

        assertEquals(65536, stack.length());
        assertTrue(stack.startsWith(IllegalStateException.class.getName()));
    }
}
