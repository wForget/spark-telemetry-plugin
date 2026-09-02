package cn.wangz.spark.telemetry.signal.profiles;

import cn.wangz.spark.telemetry.runtime.ResourceIdentity;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.config.Config;
import io.pyroscope.javaagent.config.ProfilerType;
import org.apache.spark.telemetry.config.TelemetryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ProfilePipelineTest {
    @Test
    void buildsAgentConfigAndStopsOnlyTheOwnedAgent() {
        TelemetryConfig telemetry = profileConfig();
        ResourceIdentity identity = ResourceIdentity.executor(
                telemetry, "application-7", "3");
        FakeAgent agent = new FakeAgent(false, true, false);

        ProfilePipeline pipeline = ProfilePipeline.startAsync(telemetry, identity, agent);

        assertTrue(pipeline.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(ProfilePipeline.State.OWNER, pipeline.state());
        assertEquals(1, agent.starts.get());
        Config config = agent.config;
        assertNotNull(config);
        assertEquals("orders-service", config.applicationName);
        assertEquals("http://alloy:9999", config.serverAddress);
        assertEquals(ProfilerType.ASYNC, config.profilerType);
        assertEquals(Format.JFR, config.format);
        assertEquals(EventType.WALL, config.profilingEvent);
        assertEquals(Duration.ofMillis(20), config.profilingInterval);
        assertEquals(Duration.ofSeconds(5), config.uploadInterval);
        assertEquals(1024, config.javaStackDepthMax);
        assertEquals(4, config.pushQueueCapacity);
        assertEquals("512k", config.profilingAlloc);
        assertEquals("10ms", config.profilingLock);
        assertEquals("cstack=dwarf,memlimit=64m", config.APExtraArguments);
        assertEquals("application-7", config.labels.get("spark_app_id"));
        assertEquals("3", config.labels.get("spark_executor_id"));

        pipeline.close(Duration.ofSeconds(1));
        pipeline.close(Duration.ofSeconds(1));

        assertEquals(1, agent.stops.get());
        assertEquals(ProfilePipeline.State.CLOSED, pipeline.state());
    }

    @Test
    void doesNotReconfigureOrStopAPreexistingAgent() {
        FakeAgent agent = new FakeAgent(true, true, false);
        ProfilePipeline pipeline = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-7"), agent);

        assertTrue(pipeline.awaitStartup(Duration.ofSeconds(2)));
        pipeline.close(Duration.ofSeconds(1));

        assertEquals(0, agent.starts.get());
        assertEquals(0, agent.stops.get());
        assertEquals(ProfilePipeline.State.CLOSED, pipeline.state());
    }

    @Test
    void failedStartNeverClaimsOwnership() {
        FakeAgent agent = new FakeAgent(false, false, false);
        ProfilePipeline pipeline = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-7"), agent);

        assertTrue(pipeline.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(ProfilePipeline.State.FAILED, pipeline.state());
        pipeline.close(Duration.ofSeconds(1));

        assertEquals(1, agent.starts.get());
        assertEquals(0, agent.stops.get());
    }

    @Test
    void shutdownDuringStartupBlocksReplacementUntilTheOldAgentHasStopped() {
        FakeAgent oldAgent = new FakeAgent(false, true, true);
        ProfilePipeline oldPipeline = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-old"), oldAgent);
        assertTrue(oldAgent.awaitStartEntered());

        long startedAt = System.nanoTime();
        oldPipeline.close(Duration.ofMillis(5));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        assertTrue(elapsedMillis < 500L, "close must respect its deadline");

        FakeAgent replacementAgent = new FakeAgent(false, true, false);
        ProfilePipeline replacement = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-new"),
                replacementAgent);
        assertFalse(replacement.awaitStartup(Duration.ofMillis(50)),
                "replacement must wait while the old profiler is still starting or stopping");

        oldAgent.allowStart.countDown();
        assertTrue(oldPipeline.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(1, oldAgent.stops.get());
        assertEquals(ProfilePipeline.State.CLOSED, oldPipeline.state());

        assertTrue(replacement.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(ProfilePipeline.State.OWNER, replacement.state());
        assertEquals(1, replacementAgent.starts.get());
        replacement.close(Duration.ofSeconds(1));
        assertEquals(1, replacementAgent.stops.get());
    }

    @Test
    void newPipelineWaitsForAnOwnedAgentThatIsStopping() throws Exception {
        FakeAgent oldAgent = new FakeAgent(false, true, false);
        ProfilePipeline oldPipeline = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-old"), oldAgent);
        assertTrue(oldPipeline.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(ProfilePipeline.State.OWNER, oldPipeline.state());
        oldAgent.blockStop = true;

        Thread closeOld = new Thread(() -> oldPipeline.close(Duration.ofSeconds(15)));
        closeOld.start();
        assertTrue(oldAgent.awaitStopEntered());

        FakeAgent newAgent = new FakeAgent(false, true, false);
        ProfilePipeline newPipeline = ProfilePipeline.startAsync(
                profileConfig(), ResourceIdentity.driver(profileConfig(), "application-new"), newAgent);
        assertFalse(newPipeline.awaitStartup(Duration.ofMillis(50)),
                "new owner should wait while the previous plugin owner is stopping");

        oldAgent.allowStop.countDown();
        closeOld.join(TimeUnit.SECONDS.toMillis(2));
        assertFalse(closeOld.isAlive());
        assertTrue(newPipeline.awaitStartup(Duration.ofSeconds(2)));
        assertEquals(ProfilePipeline.State.OWNER, newPipeline.state());
        assertEquals(1, newAgent.starts.get());

        newPipeline.close(Duration.ofSeconds(1));
        assertEquals(1, newAgent.stops.get());
    }

    private static TelemetryConfig profileConfig() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
        values.put(TelemetryConfig.SERVICE_NAME().key(), "orders-service");
        values.put(TelemetryConfig.PROFILE_ENDPOINT().key(), "http://alloy:9999");
        values.put(TelemetryConfig.PROFILE_EVENT().key(), "wall");
        values.put(TelemetryConfig.PROFILE_INTERVAL().key(), "20ms");
        values.put(TelemetryConfig.PROFILE_UPLOAD_INTERVAL().key(), "5s");
        values.put(TelemetryConfig.PROFILE_JAVA_STACK_DEPTH().key(), "1024");
        values.put(TelemetryConfig.PROFILES_QUEUE_CAPACITY().key(), "4");
        values.put(TelemetryConfig.PROFILE_ALLOC().key(), "512k");
        values.put(TelemetryConfig.PROFILE_LOCK().key(), "10ms");
        values.put(TelemetryConfig.ASYNC_PROFILER_EXTRA_ARGUMENTS().key(),
                "cstack=dwarf,memlimit=64m");
        return TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("orders", "application-7");
    }

    private static final class FakeAgent implements ProfilePipeline.AgentController {
        private final boolean startSucceeds;
        private final boolean blockStart;
        private final CountDownLatch startEntered = new CountDownLatch(1);
        private final CountDownLatch allowStart;
        private final CountDownLatch stopEntered = new CountDownLatch(1);
        private final CountDownLatch allowStop = new CountDownLatch(1);
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger stops = new AtomicInteger();
        private volatile boolean blockStop;
        private volatile boolean started;
        private volatile Config config;

        private FakeAgent(boolean initiallyStarted, boolean startSucceeds, boolean blockStart) {
            this.started = initiallyStarted;
            this.startSucceeds = startSucceeds;
            this.blockStart = blockStart;
            this.allowStart = new CountDownLatch(blockStart ? 1 : 0);
        }

        @Override public boolean isStarted() { return started; }

        @Override public void start(Config config) {
            this.config = config;
            starts.incrementAndGet();
            startEntered.countDown();
            if (blockStart) {
                try {
                    allowStart.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            started = startSucceeds;
        }

        @Override public void stop() {
            stops.incrementAndGet();
            stopEntered.countDown();
            if (blockStop) {
                try {
                    allowStop.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            started = false;
        }

        private boolean awaitStartEntered() {
            try {
                return startEntered.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private boolean awaitStopEntered() {
            try {
                return stopEntered.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}
