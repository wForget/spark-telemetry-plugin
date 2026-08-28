package cn.wangz.spark.telemetry.signal.traces;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskSamplerTest {
    @Test
    void alwaysKeepsFailuresAndSlowTasks() {
        assertTrue(TaskSampler.shouldTrace(1L, true, 1L, 100L, 0.0d));
        assertTrue(TaskSampler.shouldTrace(2L, false, 100L, 100L, 0.0d));
    }

    @Test
    void normalSamplingIsStableAndBounded() {
        int retained = 0;
        for (long task = 0; task < 10000; task++) {
            boolean first = TaskSampler.shouldTrace(task, false, 1L, 100L, 0.01d);
            assertEquals(first, TaskSampler.shouldTrace(task, false, 1L, 100L, 0.01d));
            if (first) retained++;
        }
        assertTrue(retained >= 70 && retained <= 130, "retained=" + retained);
    }
}
