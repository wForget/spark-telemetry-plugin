package cn.wangz.spark.telemetry.signal.profiles;

import io.pyroscope.javaagent.api.ProfilerApi;
import io.pyroscope.javaagent.api.ProfilerApiHolder;
import io.pyroscope.javaagent.api.ProfilerScopedContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PyroscopeProfileContextTest {
    @Test
    void opensStageLabelsAndClosesTheDelegateOnlyOnce() {
        AtomicReference<Map<String, String>> opened =
                new AtomicReference<Map<String, String>>();
        AtomicInteger closes = new AtomicInteger();
        ProfileContext context = PyroscopeProfileContext.create(labels -> {
            opened.set(labels);
            return closes::incrementAndGet;
        });

        ProfileScope scope = context.openStage(12, 3);

        assertEquals("12", opened.get().get(PyroscopeProfileContext.STAGE_ID));
        assertEquals("3", opened.get().get(PyroscopeProfileContext.STAGE_ATTEMPT));
        assertThrows(UnsupportedOperationException.class,
                () -> opened.get().put("unexpected", "value"));
        scope.close();
        scope.close();
        assertEquals(1, closes.get());
    }

    @Test
    void openAndCloseFailuresAreFailOpen() {
        ProfileContext openFailure = PyroscopeProfileContext.create(labels -> {
            throw new IllegalStateException("open failed");
        });
        ProfileContext closeFailure = PyroscopeProfileContext.create(labels -> () -> {
            throw new IllegalStateException("close failed");
        });

        openFailure.openStage(1, 0).close();
        ProfileScope scope = closeFailure.openStage(2, 1);
        scope.close();
        scope.close();
    }

    @Test
    void linkageFailuresAreFailOpen() {
        ProfileContext context = PyroscopeProfileContext.create(labels -> {
            throw new NoClassDefFoundError("optional profiler API unavailable");
        });

        context.openStage(1, 0).close();
    }

    @Test
    void ownerGateRejectsExternalAndStoppingAgentsAndTracksActiveScopes() {
        ProfilerApi previous = ProfilerApiHolder.INSTANCE.getAndSet(new FakeProfilerApi());
        PyroscopeProfileContext.beginOwnerShutdown();
        try {
            ProfileContext context = PyroscopeProfileContext.create();
            assertSame(ProfileScope.NONE, context.openStage(4, 0),
                    "a profiler that is not owned by this plugin must remain untouched");

            PyroscopeProfileContext.activateOwner();
            ProfileScope scope = context.openStage(4, 2);
            assertFalse(PyroscopeProfileContext.awaitScopes(System.nanoTime()),
                    "shutdown must observe the active task scope");

            PyroscopeProfileContext.beginOwnerShutdown();
            assertSame(ProfileScope.NONE, context.openStage(5, 0),
                    "no new task scope may cross the shutdown gate");
            scope.close();
            assertTrue(PyroscopeProfileContext.awaitScopes(System.nanoTime()));
        } finally {
            PyroscopeProfileContext.beginOwnerShutdown();
            ProfilerApiHolder.INSTANCE.set(previous);
        }
    }

    private static final class FakeProfilerApi implements ProfilerApi {
        @Override public void startProfiling() {}
        @Override public boolean isProfilingStarted() { return true; }
        @Override public ProfilerScopedContext createScopedContext(Map<String, String> labels) {
            return new ProfilerScopedContext() {
                @Override public void forEachLabel(BiConsumer<String, String> consumer) {
                    labels.forEach(consumer);
                }
                @Override public void close() {}
            };
        }
        @Override public void setTracingContext(long first, long second) {}
        @Override public long registerConstant(String constant) { return 0L; }
    }
}
