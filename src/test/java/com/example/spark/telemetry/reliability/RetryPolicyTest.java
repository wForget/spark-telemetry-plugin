package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    @Test
    void computesExponentialBackoffAndCapsDelay() {
        RetryPolicy policy = new RetryPolicy(100, 1_000, 2.0, 0.0, 10, 10_000, () -> 0.5);

        assertEquals(100L, policy.delayMillis(1));
        assertEquals(200L, policy.delayMillis(2));
        assertEquals(800L, policy.delayMillis(4));
        assertEquals(1_000L, policy.delayMillis(20));
    }

    @Test
    void injectedJitterIsDeterministicAndBounded() {
        RetryPolicy low = new RetryPolicy(100, 1_000, 2.0, 0.25, 3, 10_000, () -> 0.0);
        RetryPolicy high = new RetryPolicy(100, 1_000, 2.0, 0.25, 3, 10_000, () -> 0.999999);

        assertEquals(75L, low.delayMillis(1));
        assertEquals(125L, high.delayMillis(1));
    }

    @Test
    void enforcesRetryCountAndElapsedDeadline() {
        RetryPolicy policy = new RetryPolicy(100, 500, 2.0, 0.0, 2, 250, () -> 0.5);

        assertTrue(policy.shouldRetry(1, 0));
        assertTrue(policy.shouldRetry(2, 100));
        assertEquals(-1L, policy.nextDelayMillis(2, 100));
        assertFalse(policy.shouldRetry(3, 0));
        assertEquals(-1L, policy.nextDelayMillis(3, 0));
        assertThrows(IllegalArgumentException.class, () -> policy.delayMillis(0));
    }
}
