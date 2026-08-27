package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    @Test
    void opensAfterThresholdAndAllowsOneHalfOpenProbe() {
        AtomicLong time = new AtomicLong();
        CircuitBreaker breaker = new CircuitBreaker(2, 10, TimeUnit.NANOSECONDS, time::get);

        CircuitBreaker.Permit first = breaker.tryAcquirePermit();
        assertTrue(first != null);
        first.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
        CircuitBreaker.Permit second = breaker.tryAcquirePermit();
        assertTrue(second != null);
        second.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
        assertEquals(null, breaker.tryAcquirePermit());

        time.set(10L);
        CircuitBreaker.Permit probe = breaker.tryAcquirePermit();
        assertTrue(probe != null);
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());
        assertEquals(null, breaker.tryAcquirePermit());

        probe.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
        assertTrue(breaker.tryAcquirePermit() != null);
    }

    @Test
    void failedProbeReopensForAnotherFullInterval() {
        AtomicLong time = new AtomicLong();
        CircuitBreaker breaker = new CircuitBreaker(1, 10, TimeUnit.NANOSECONDS, time::get);
        breaker.tryAcquirePermit().recordFailure();
        time.set(10L);
        CircuitBreaker.Permit probe = breaker.tryAcquirePermit();
        assertTrue(probe != null);

        probe.recordFailure();

        time.set(19L);
        assertEquals(null, breaker.tryAcquirePermit());
        time.set(20L);
        assertTrue(breaker.tryAcquirePermit() != null);
    }

    @Test
    void ignoresLateResultsFromAnOlderGeneration() {
        AtomicLong time = new AtomicLong();
        CircuitBreaker breaker = new CircuitBreaker(1, 10, TimeUnit.NANOSECONDS, time::get);
        CircuitBreaker.Permit openingFailure = breaker.tryAcquirePermit();
        CircuitBreaker.Permit staleSuccess = breaker.tryAcquirePermit();
        openingFailure.recordFailure();
        time.set(10L);
        CircuitBreaker.Permit probe = breaker.tryAcquirePermit();

        staleSuccess.recordSuccess();
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());
        probe.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
    }
}
