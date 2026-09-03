package cn.wangz.spark.telemetry.signal.profiles;

import io.pyroscope.javaagent.api.ProfilerApi;
import io.pyroscope.javaagent.api.ProfilerApiHolder;
import io.pyroscope.javaagent.api.ProfilerScopedContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

/** Pyroscope dynamic labels kept separate from profiler lifecycle ownership. */
public final class PyroscopeProfileContext implements ProfileContext {
    static final String STAGE_ID = "spark_stage_id";
    static final String STAGE_ATTEMPT = "spark_stage_attempt";
    private static final Object GATE = new Object();
    private static boolean acceptingScopes;
    private static int activeScopes;

    private final ContextController controller;

    private PyroscopeProfileContext(ContextController controller) {
        this.controller = controller;
    }

    public static ProfileContext create() {
        return new PyroscopeProfileContext(new RealContextController());
    }

    static ProfileContext create(ContextController controller) {
        return new PyroscopeProfileContext(controller);
    }

    static void activateOwner() {
        synchronized (GATE) {
            acceptingScopes = true;
            GATE.notifyAll();
        }
    }

    static void beginOwnerShutdown() {
        synchronized (GATE) {
            acceptingScopes = false;
            GATE.notifyAll();
        }
    }

    static boolean awaitScopes(long deadlineNanos) {
        boolean interrupted = false;
        try {
            synchronized (GATE) {
                while (activeScopes > 0) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0L) return false;
                    try {
                        TimeUnit.NANOSECONDS.timedWait(GATE, remaining);
                    } catch (InterruptedException interruption) {
                        interrupted = true;
                        return false;
                    }
                }
                return true;
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    @Override
    public ProfileScope openStage(int stageId, int stageAttempt) {
        try {
            if (!controller.isAvailable()) return ProfileScope.NONE;
            Map<String, String> labels = new LinkedHashMap<String, String>();
            labels.put(STAGE_ID, String.valueOf(stageId));
            labels.put(STAGE_ATTEMPT, String.valueOf(stageAttempt));
            ProfileScope scope = controller.open(Collections.unmodifiableMap(labels));
            if (scope == null || scope == ProfileScope.NONE) return ProfileScope.NONE;
            return new SafeScope(scope);
        } catch (RuntimeException ignored) {
            return ProfileScope.NONE;
        } catch (LinkageError ignored) {
            return ProfileScope.NONE;
        }
    }

    interface ContextController {
        default boolean isAvailable() { return true; }
        ProfileScope open(Map<String, String> labels);
    }

    private static final class RealContextController implements ContextController {
        @Override
        public boolean isAvailable() {
            synchronized (GATE) {
                return acceptingScopes;
            }
        }

        @Override
        public ProfileScope open(Map<String, String> labels) {
            synchronized (GATE) {
                // Only this classloader's plugin-owned agent opens the gate. A pre-existing
                // external agent and an owner in an inaccessible classloader remain untouched.
                if (!acceptingScopes) return ProfileScope.NONE;
                ProfilerApi api = ProfilerApiHolder.INSTANCE.get();
                if (api == null || !api.isProfilingStarted()) return ProfileScope.NONE;
                final ProfilerScopedContext context = api.createScopedContext(labels);
                if (context == null) return ProfileScope.NONE;
                activeScopes++;
                return new ProfileScope() {
                    @Override public void close() {
                        try {
                            context.close();
                        } finally {
                            releaseScope();
                        }
                    }
                };
            }
        }
    }

    private static void releaseScope() {
        synchronized (GATE) {
            if (activeScopes > 0) activeScopes--;
            GATE.notifyAll();
        }
    }

    private static final class SafeScope implements ProfileScope {
        private final ProfileScope delegate;
        private final AtomicBoolean closed = new AtomicBoolean();

        private SafeScope(ProfileScope delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            try {
                delegate.close();
            } catch (RuntimeException ignored) {
                // Profiling must never alter task success or failure.
            } catch (LinkageError ignored) {
                // An optional-agent ABI failure is isolated to this profile scope.
            }
        }
    }
}
