package cn.wangz.spark.telemetry.signal.profiles;

import cn.wangz.spark.telemetry.runtime.ResourceIdentity;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.api.Logger.Level;
import io.pyroscope.javaagent.config.Config;
import io.pyroscope.javaagent.config.ProfilerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.telemetry.config.TelemetryConfig;

import java.time.Duration;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Owns one programmatically started Pyroscope agent without putting native startup on a Spark
 * lifecycle or task thread. All direct references to the optional Pyroscope dependency live here.
 */
public final class ProfilePipeline implements ProfileLifecycle {
    private static final Logger LOG = LogManager.getLogger(ProfilePipeline.class);

    private final Object stateLock = new Object();
    private final AgentController agent;
    private final Config agentConfig;
    private final JvmOwnership ownership = new JvmOwnership();
    private final CountDownLatch startupFinished = new CountDownLatch(1);
    private volatile State state = State.STARTING;
    private volatile boolean closeRequested;
    private Thread startupThread;
    private Thread stopThread;

    private ProfilePipeline(Config agentConfig, AgentController agent) {
        this.agentConfig = agentConfig;
        this.agent = agent;
    }

    public static ProfileLifecycle startAsync(TelemetryConfig config, ResourceIdentity identity) {
        return startAsync(config, identity, new RealAgentController());
    }

    static ProfilePipeline startAsync(
            TelemetryConfig config, ResourceIdentity identity, AgentController agent) {
        ProfilePipeline pipeline = new ProfilePipeline(buildConfig(config, identity), agent);
        pipeline.startWorker();
        return pipeline;
    }

    static Config buildConfig(TelemetryConfig config, ResourceIdentity identity) {
        Config.Builder builder = new Config.Builder()
                .setApplicationName(config.serviceName())
                .setServerAddress(config.profileEndpoint())
                .setProfilerType(ProfilerType.ASYNC)
                .setFormat(Format.JFR)
                .setProfilingEvent(EventType.valueOf(config.profileEvent()))
                .setProfilingInterval(config.profileInterval())
                .setUploadInterval(config.profileUploadInterval())
                .setJavaStackDepthMax(config.profileJavaStackDepth())
                .setPushQueueCapacity(config.profilesQueueCapacity())
                .setLabels(identity.profileLabels())
                .setLogLevel(Level.WARN)
                .setAPLogLevel("WARN")
                .setGcBeforeDump(false);
        if (!config.profileAlloc().isEmpty()) builder.setProfilingAlloc(config.profileAlloc());
        if (!config.profileLock().isEmpty()) builder.setProfilingLock(config.profileLock());
        if (!config.asyncProfilerExtraArguments().isEmpty()) {
            builder.setAPExtraArguments(config.asyncProfilerExtraArguments());
        }
        return builder.build();
    }

    private void startWorker() {
        Thread worker = new Thread(new Runnable() {
            @Override public void run() { startAgent(); }
        }, "spark-telemetry-profile-start");
        worker.setDaemon(true);
        synchronized (stateLock) {
            startupThread = worker;
        }
        worker.start();
    }

    private void startAgent() {
        try {
            synchronized (stateLock) {
                if (closeRequested) {
                    state = State.CLOSED;
                    return;
                }
            }
            ClaimResult claim = ownership.claim(agent);
            if (claim != ClaimResult.CLAIMED) {
                synchronized (stateLock) {
                    state = closeRequested ? State.CLOSED : State.NON_OWNER;
                }
                LOG.warn("Pyroscope is already running or owned by another classloader; " +
                        "Spark telemetry will not reconfigure or stop it");
                return;
            }
            synchronized (stateLock) {
                if (closeRequested) {
                    state = State.CLOSED;
                    ownership.release();
                    return;
                }
            }

            agent.start(agentConfig);
            boolean started = agent.isStarted();
            boolean stopImmediately = false;
            synchronized (stateLock) {
                if (!started) {
                    state = State.FAILED;
                    ownership.release();
                } else if (closeRequested) {
                    state = State.STOPPING;
                    ownership.markStopping();
                    stopImmediately = true;
                } else {
                    state = State.OWNER;
                    ownership.markActive();
                }
            }
            if (!started) {
                LOG.warn("Pyroscope did not start; profiling is disabled for this Spark JVM");
            } else if (stopImmediately) {
                stopAgentOnCurrentThread();
            }
        } catch (RuntimeException failure) {
            failStartup(failure);
        } catch (LinkageError failure) {
            failStartup(failure);
        } finally {
            startupFinished.countDown();
        }
    }

    private void failStartup(Throwable failure) {
        ownership.release();
        synchronized (stateLock) {
            state = closeRequested ? State.CLOSED : State.FAILED;
        }
        LOG.warn("Pyroscope startup failed; profiling is disabled for this Spark JVM: {}",
                failure.toString());
    }

    @Override
    public void close(Duration timeout) {
        final long deadline = System.nanoTime() + Math.max(0L, timeout.toNanos());
        Thread localStartup;
        Thread localStop;
        boolean launchStop = false;
        synchronized (stateLock) {
            closeRequested = true;
            if (state == State.OWNER) {
                state = State.STOPPING;
                ownership.markStopping();
                stopThread = new Thread(new Runnable() {
                    @Override public void run() { stopAgent(); }
                }, "spark-telemetry-profile-stop");
                stopThread.setDaemon(true);
                launchStop = true;
            } else if (state == State.STARTING) {
                ownership.markStopping();
            } else if (state == State.NON_OWNER || state == State.FAILED) {
                state = State.CLOSED;
            }
            localStartup = startupThread;
            localStop = stopThread;
        }
        if (launchStop) localStop.start();
        joinUntil(localStartup, deadline);
        synchronized (stateLock) {
            localStop = stopThread;
        }
        joinUntil(localStop, deadline);
        if ((localStartup != null && localStartup.isAlive()) ||
                (localStop != null && localStop.isAlive())) {
            LOG.warn("Pyroscope shutdown exceeded the Spark telemetry shutdown deadline");
        }
    }

    private void stopAgent() {
        stopAgentOnCurrentThread();
    }

    private void stopAgentOnCurrentThread() {
        try {
            agent.stop();
        } catch (RuntimeException failure) {
            LOG.warn("Pyroscope shutdown failed: {}", failure.toString());
        } catch (LinkageError failure) {
            LOG.warn("Pyroscope shutdown failed: {}", failure.toString());
        } finally {
            ownership.release();
            synchronized (stateLock) {
                state = State.CLOSED;
            }
        }
    }

    boolean awaitStartup(Duration timeout) {
        try {
            return startupFinished.await(Math.max(0L, timeout.toNanos()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    State state() { return state; }

    private static void joinUntil(Thread thread, long deadline) {
        if (thread == null) return;
        boolean interrupted = false;
        try {
            while (thread.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) return;
                try {
                    TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
                } catch (InterruptedException interruption) {
                    interrupted = true;
                    return;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    interface AgentController {
        boolean isStarted();
        void start(Config config);
        void stop();
    }

    private static final class RealAgentController implements AgentController {
        @Override public boolean isStarted() { return PyroscopeAgent.isStarted(); }
        @Override public void start(Config config) { PyroscopeAgent.start(config); }
        @Override public void stop() { PyroscopeAgent.stop(); }
    }

    /** Coordinates profiler ownership through the JVM-global system Properties object. */
    private static final class JvmOwnership {
        private static final String OWNER_KEY =
                "cn.wangz.spark.telemetry.pyroscope.owner";
        private static final String STATE_KEY =
                "cn.wangz.spark.telemetry.pyroscope.owner.state";
        private static final String ACTIVE = "active";
        private static final String STARTING = "starting";
        private static final String STOPPING = "stopping";
        private static final long HANDOFF_WAIT_NANOS = TimeUnit.SECONDS.toNanos(12L);

        private final String token = UUID.randomUUID().toString();
        private volatile boolean claimed;

        ClaimResult claim(AgentController agent) {
            final Properties properties;
            try {
                properties = System.getProperties();
            } catch (SecurityException denied) {
                LOG.warn("Cannot coordinate Pyroscope ownership through system properties; " +
                        "profiling is disabled for safety");
                return ClaimResult.OCCUPIED;
            }
            long deadline = System.nanoTime() + HANDOFF_WAIT_NANOS;
            synchronized (properties) {
                while (properties.get(OWNER_KEY) != null &&
                        (STARTING.equals(properties.get(STATE_KEY)) ||
                                STOPPING.equals(properties.get(STATE_KEY)))) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) return ClaimResult.OCCUPIED;
                    try {
                        TimeUnit.NANOSECONDS.timedWait(properties, remaining);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return ClaimResult.OCCUPIED;
                    }
                }
                if (properties.get(OWNER_KEY) != null) return ClaimResult.OCCUPIED;
                if (systemLoaderAgentStarted() || agent.isStarted()) return ClaimResult.OCCUPIED;
                properties.put(OWNER_KEY, token);
                properties.put(STATE_KEY, STARTING);
                claimed = true;
                return ClaimResult.CLAIMED;
            }
        }

        void markActive() {
            updateState(ACTIVE);
        }

        void markStopping() {
            updateState(STOPPING);
        }

        void release() {
            if (!claimed) return;
            try {
                Properties properties = System.getProperties();
                synchronized (properties) {
                    if (!token.equals(properties.get(OWNER_KEY))) return;
                    properties.remove(OWNER_KEY);
                    properties.remove(STATE_KEY);
                    claimed = false;
                    properties.notifyAll();
                }
            } catch (SecurityException denied) {
                // The claim already succeeded; retain the marker rather than risk a second profiler.
            }
        }

        private void updateState(String state) {
            if (!claimed) return;
            try {
                Properties properties = System.getProperties();
                synchronized (properties) {
                    if (!token.equals(properties.get(OWNER_KEY))) return;
                    properties.put(STATE_KEY, state);
                    properties.notifyAll();
                }
            } catch (SecurityException denied) {
                // The claim already succeeded; retain the marker rather than risk a second profiler.
            }
        }

        private static boolean systemLoaderAgentStarted() {
            try {
                Class<?> systemAgent = Class.forName(
                        "io.pyroscope.javaagent.PyroscopeAgent", false,
                        ClassLoader.getSystemClassLoader());
                if (systemAgent == PyroscopeAgent.class) return false;
                Method isStarted = systemAgent.getMethod("isStarted");
                return Boolean.TRUE.equals(isStarted.invoke(null));
            } catch (ClassNotFoundException absent) {
                return false;
            } catch (ReflectiveOperationException uncertain) {
                LOG.warn("A system-loader Pyroscope agent was found but could not be inspected; " +
                        "plugin-managed profiling is disabled for safety");
                return true;
            } catch (LinkageError uncertain) {
                return true;
            } catch (SecurityException uncertain) {
                return true;
            }
        }
    }

    enum ClaimResult { CLAIMED, OCCUPIED }
    enum State { STARTING, OWNER, NON_OWNER, FAILED, STOPPING, CLOSED }
}
