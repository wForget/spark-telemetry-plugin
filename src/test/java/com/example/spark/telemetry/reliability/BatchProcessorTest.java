package com.example.spark.telemetry.reliability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BatchProcessorTest {

    @Test
    void drainsFullBatchesAndUsesImmutableExportList() throws Exception {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(10);
        List<Integer> exported = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch exportedAll = new CountDownLatch(4);
        AtomicInteger immutableChecks = new AtomicInteger();
        BatchProcessor<Integer> processor = new BatchProcessor<Integer>(
                queue,
                2,
                1,
                TimeUnit.SECONDS,
                batch -> {
                    assertThrows(UnsupportedOperationException.class, () -> batch.add(99));
                    immutableChecks.incrementAndGet();
                    exported.addAll(batch);
                    for (int ignored : batch) {
                        exportedAll.countDown();
                    }
                });
        processor.start();

        processor.offer(1);
        processor.offer(2);
        processor.offer(3);
        processor.offer(4);

        assertTrue(exportedAll.await(2, TimeUnit.SECONDS));
        assertTrue(processor.shutdown(1, TimeUnit.SECONDS));
        assertEquals(4, exported.size());
        assertEquals(2, immutableChecks.get());
    }

    @Test
    void flushesPartialBatchOnTimer() throws Exception {
        CountDownLatch exported = new CountDownLatch(1);
        BatchProcessor<Integer> processor = new BatchProcessor<Integer>(
                new BoundedSignalQueue<Integer>(4),
                4,
                30,
                TimeUnit.MILLISECONDS,
                batch -> exported.countDown());
        processor.start();

        assertTrue(processor.offer(1));

        assertTrue(exported.await(1, TimeUnit.SECONDS));
        assertTrue(processor.shutdown(1, TimeUnit.SECONDS));
    }

    @Test
    void isolatesExporterAndListenerFailures() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch secondAttempt = new CountDownLatch(1);
        BatchProcessor.Listener throwingListener = new BatchProcessor.Listener() {
            @Override
            public void onExportFailure(int batchSize, Throwable failure, long durationNanos) {
                throw new IllegalStateException("listener failed");
            }
        };
        BatchProcessor<Integer> processor = new BatchProcessor<Integer>(
                new BoundedSignalQueue<Integer>(4),
                1,
                10,
                TimeUnit.MILLISECONDS,
                batch -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new Exception("export failed");
                    }
                    secondAttempt.countDown();
                },
                throwingListener,
                null);
        processor.start();

        processor.offer(1);
        processor.offer(2);

        assertTrue(secondAttempt.await(1, TimeUnit.SECONDS));
        assertTrue(processor.shutdown(1, TimeUnit.SECONDS));
        assertEquals(2, attempts.get());
    }

    @Test
    void shutdownIsBoundedWhenExporterHangs() throws Exception {
        CountDownLatch exportStarted = new CountDownLatch(1);
        CountDownLatch releaseExporter = new CountDownLatch(1);
        CountDownLatch discardNotified = new CountDownLatch(1);
        AtomicInteger discarded = new AtomicInteger();
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(4);
        BatchProcessor.Listener listener = new BatchProcessor.Listener() {
            @Override
            public void onDiscard(int itemCount) {
                discarded.addAndGet(itemCount);
                discardNotified.countDown();
            }
        };
        BatchProcessor<Integer> processor = new BatchProcessor<Integer>(
                queue,
                1,
                5,
                TimeUnit.MILLISECONDS,
                batch -> {
                    exportStarted.countDown();
                    while (releaseExporter.getCount() > 0) {
                        try {
                            releaseExporter.await();
                        } catch (InterruptedException ignored) {
                            // Simulate an exporter that ignores interruption.
                        }
                    }
                },
                listener,
                null);
        processor.start();
        processor.offer(1);
        processor.offer(2);
        assertTrue(exportStarted.await(1, TimeUnit.SECONDS));

        long start = System.nanoTime();
        assertFalse(processor.shutdown(30, TimeUnit.MILLISECONDS));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < 500L);
        assertFalse(processor.offer(3));
        assertTrue(discardNotified.await(1, TimeUnit.SECONDS));
        assertEquals(1L, queue.droppedCount());
        releaseExporter.countDown();
        assertTrue(processor.awaitTermination(1, TimeUnit.SECONDS));
        assertEquals(1, discarded.get());
    }

    @Test
    void startFailureClosesAndDiscardsPreStartItems() {
        BoundedSignalQueue<Integer> queue = new BoundedSignalQueue<Integer>(2);
        BatchProcessor<Integer> processor = new BatchProcessor<Integer>(
                queue,
                1,
                1,
                TimeUnit.SECONDS,
                batch -> { },
                null,
                runnable -> new Thread(runnable) {
                    @Override
                    public synchronized void start() {
                        throw new IllegalThreadStateException("injected start failure");
                    }
                });
        assertTrue(processor.offer(1));

        assertFalse(processor.start());

        assertEquals(BatchProcessor.State.TERMINATED, processor.state());
        assertFalse(processor.isAccepting());
        assertEquals(0, queue.size());
        assertEquals(1L, queue.droppedCount());
        assertFalse(processor.shutdown(1, TimeUnit.SECONDS));
    }
}
