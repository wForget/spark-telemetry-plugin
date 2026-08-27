package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedSignalQueueTest {

    @Test
    void dropsIncomingByDefaultAndRemainsBounded() {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(2);

        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertFalse(queue.offer(3));
        assertEquals(2, queue.size());
        assertEquals(1L, queue.droppedCount());
    }

    @Test
    void canAtomicallyReplaceOldestElement() {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(
                2, BoundedSignalQueue.dropOldest());
        queue.offer(1);
        queue.offer(2);

        assertTrue(queue.offer(3));

        List<Integer> drained = new ArrayList<Integer>();
        assertEquals(2, queue.drainTo(drained, 10));
        assertEquals(Arrays.asList(2, 3), drained);
        assertEquals(1L, queue.droppedCount());
    }

    @Test
    void closeRejectsFutureOffersButAllowsDrain() {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(2);
        queue.offer(1);

        queue.close();

        assertFalse(queue.offer(2));
        assertFalse(queue.isAccepting());
        assertEquals(Integer.valueOf(1), queue.poll());
    }
}
