package com.example.spark.telemetry.runtime;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

import java.util.concurrent.TimeUnit;

/** Fixed, bounded-cardinality metric instruments. */
final class MetricPipeline {
    private final LongCounter jobsCompleted;
    private final LongCounter stagesCompleted;
    private final LongCounter tasksCompleted;
    private final LongHistogram jobDuration;
    private final LongHistogram stageDuration;
    private final LongHistogram taskDuration;
    private final LongUpDownCounter activeJobs;
    private final LongUpDownCounter activeStages;
    private final LongUpDownCounter activeTasks;
    private final LongUpDownCounter activeExecutors;

    MetricPipeline(Meter meter) {
        jobsCompleted = counter(meter, "spark.jobs.completed", "Completed Spark jobs");
        stagesCompleted = counter(meter, "spark.stages.completed", "Completed Spark stages");
        tasksCompleted = counter(meter, "spark.tasks.completed", "Completed Spark tasks");
        jobDuration = histogram(meter, "spark.job.duration", "Spark job duration");
        stageDuration = histogram(meter, "spark.stage.duration", "Spark stage duration");
        taskDuration = histogram(meter, "spark.task.duration", "Spark task duration");
        activeJobs = upDown(meter, "spark.jobs.active", "Active Spark jobs");
        activeStages = upDown(meter, "spark.stages.active", "Active Spark stages");
        activeTasks = upDown(meter, "spark.tasks.active", "Active Spark tasks");
        activeExecutors = upDown(meter, "spark.executors.active", "Active Spark executors");
    }

    void jobStarted() { activeJobs.add(1); }
    void jobEnded(long durationMillis, String outcome) {
        activeJobs.add(-1);
        Attributes attributes = outcome(outcome);
        jobsCompleted.add(1, attributes);
        jobDuration.record(Math.max(0L, durationMillis), attributes);
    }
    void stageStarted() { activeStages.add(1); }
    void stageEnded(long durationMillis, String outcome) {
        activeStages.add(-1);
        Attributes attributes = outcome(outcome);
        stagesCompleted.add(1, attributes);
        stageDuration.record(Math.max(0L, durationMillis), attributes);
    }
    void taskStarted() { activeTasks.add(1); }
    void taskEnded(long durationNanos, String outcome) {
        activeTasks.add(-1);
        Attributes attributes = outcome(outcome);
        tasksCompleted.add(1, attributes);
        taskDuration.record(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, durationNanos)), attributes);
    }
    void executorAdded() { activeExecutors.add(1); }
    void executorRemoved() { activeExecutors.add(-1); }

    private static Attributes outcome(String outcome) {
        return Attributes.builder().put("outcome", outcome == null ? "unknown" : outcome).build();
    }
    private static LongCounter counter(Meter meter, String name, String description) {
        return meter.counterBuilder(name).setDescription(description).setUnit("{event}").build();
    }
    private static LongHistogram histogram(Meter meter, String name, String description) {
        return meter.histogramBuilder(name).ofLongs().setDescription(description).setUnit("ms").build();
    }
    private static LongUpDownCounter upDown(Meter meter, String name, String description) {
        return meter.upDownCounterBuilder(name).setDescription(description).setUnit("{event}").build();
    }
}
