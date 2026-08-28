package cn.wangz.spark.telemetry.signal.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkMetricProducerTest {
    @Test
    void readsCurrentRegistryWithoutCreatingOrCachingInstruments() {
        MetricRegistry registry = new MetricRegistry();
        Counter counter = registry.counter("spark.executor.runningTasks");
        counter.inc(3L);
        SparkMetricProducer producer = new SparkMetricProducer(registry);

        Map<String, MetricData> first = byName(producer.produce(Resource.empty()));
        assertEquals(1, registry.getMetrics().size());
        assertEquals(3L, first.get("spark.executor.runningTasks")
                .getLongGaugeData().getPoints().iterator().next().getValue());

        registry.register("spark.executor.dynamicGauge", new Gauge<Integer>() {
            @Override public Integer getValue() { return Integer.valueOf(7); }
        });
        Map<String, MetricData> second = byName(producer.produce(Resource.empty()));
        assertEquals(7L, second.get("spark.executor.dynamicGauge")
                .getLongGaugeData().getPoints().iterator().next().getValue());
        assertFalse(registry.getMetrics().containsKey("spark.jobs.completed"));
    }

    @Test
    void expandsSparkMetricTypesWithStableUnits() {
        MetricRegistry registry = new MetricRegistry();
        Meter meter = registry.meter("spark.events");
        meter.mark(2L);
        Histogram histogram = registry.histogram("spark.payloadSize");
        histogram.update(10);
        histogram.update(20);
        Timer timer = registry.timer("spark.taskTime");
        timer.update(2L, TimeUnit.MILLISECONDS);

        Map<String, MetricData> metrics =
                byName(new SparkMetricProducer(registry).produce(Resource.empty()));

        assertEquals(2L, metrics.get("spark.events.count")
                .getLongSumData().getPoints().iterator().next().getValue());
        assertTrue(metrics.get("spark.events.count").getLongSumData().isMonotonic());
        assertEquals(2L, metrics.get("spark.payloadSize.count")
                .getLongSumData().getPoints().iterator().next().getValue());
        assertEquals(20.0d, metrics.get("spark.payloadSize.max")
                .getDoubleGaugeData().getPoints().iterator().next().getValue());
        assertEquals("ms", metrics.get("spark.taskTime.duration.max").getUnit());
        assertEquals(2.0d, metrics.get("spark.taskTime.duration.max")
                .getDoubleGaugeData().getPoints().iterator().next().getValue(), 0.0001d);
    }

    @Test
    void skipsUnsupportedAndFailingGaugesWithoutDroppingOthers() {
        MetricRegistry registry = new MetricRegistry();
        registry.register("string", new Gauge<String>() {
            @Override public String getValue() { return "not numeric"; }
        });
        registry.register("broken", new Gauge<Integer>() {
            @Override public Integer getValue() { throw new IllegalStateException("broken"); }
        });
        registry.register("healthy", new Gauge<Boolean>() {
            @Override public Boolean getValue() { return Boolean.TRUE; }
        });

        Map<String, MetricData> metrics =
                byName(new SparkMetricProducer(registry).produce(Resource.empty()));

        assertEquals(1, metrics.size());
        assertTrue(metrics.containsKey("healthy"));
    }

    private static Map<String, MetricData> byName(Collection<MetricData> metrics) {
        Map<String, MetricData> result = new HashMap<String, MetricData>();
        for (MetricData metric : metrics) result.put(metric.getName(), metric);
        return result;
    }
}
