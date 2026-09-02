package cn.wangz.spark.telemetry.runtime;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.telemetry.config.TelemetryConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProfileDependencyIsolationTest {
    @Test
    void missingProvidedAgentOnlyDisablesProfiles() throws Exception {
        Map<String, String> values = new HashMap<String, String>();
        values.put(TelemetryConfig.METRICS_ENABLED().key(), "false");
        values.put(TelemetryConfig.LOGS_ENABLED().key(), "false");
        values.put(TelemetryConfig.TRACES_ENABLED().key(), "false");
        values.put(TelemetryConfig.PROFILES_ENABLED().key(), "true");
        TelemetryConfig config = TelemetryConfig.from(values, new HashMap<String, String>())
                .withApplication("isolation-test", "application-isolation");
        ResourceIdentity identity = ResourceIdentity.driver(config, "application-isolation");

        URL classes = TelemetryRuntime.class.getProtectionDomain().getCodeSource().getLocation();
        try (FilteringLoader loader = new FilteringLoader(
                new URL[] {classes}, TelemetryRuntime.class.getClassLoader())) {
            Class<?> isolatedRuntime = Class.forName(
                    TelemetryRuntime.class.getName(), true, loader);
            Method create = isolatedRuntime.getMethod(
                    "create", TelemetryConfig.class, ResourceIdentity.class, MetricRegistry.class);

            Object runtime = create.invoke(null, config, identity, null);

            assertNotNull(runtime);
            isolatedRuntime.getMethod("close", Duration.class)
                    .invoke(runtime, Duration.ofMillis(100));
        }
    }

    private static final class FilteringLoader extends URLClassLoader {
        private FilteringLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
            if (name.startsWith("io.pyroscope.")) {
                throw new ClassNotFoundException("Intentionally hidden in dependency isolation test: " + name);
            }
            if (isChildFirst(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) loaded = findClass(name);
                if (resolve) resolveClass(loaded);
                return loaded;
            }
            return super.loadClass(name, resolve);
        }

        private static boolean isChildFirst(String name) {
            return name.equals(TelemetryRuntime.class.getName()) ||
                    name.startsWith(TelemetryRuntime.class.getName() + "$") ||
                    name.startsWith("cn.wangz.spark.telemetry.signal.profiles.");
        }
    }
}
