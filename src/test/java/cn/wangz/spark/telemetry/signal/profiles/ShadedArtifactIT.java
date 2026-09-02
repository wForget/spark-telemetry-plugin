package cn.wangz.spark.telemetry.signal.profiles;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies the final shaded artifact after Maven's package phase. */
class ShadedArtifactIT {
    private static final String[] NATIVE_LIBRARIES = {
            "libasyncProfiler-linux-x64.so",
            "libasyncProfiler-linux-arm64.so",
            "libasyncProfiler-macos.so"
    };

    @Test
    void embedsAnUnrelocatedSelfContainedPyroscopeDistribution() throws Exception {
        Path artifact = pluginArtifact();
        assertTrue(Files.isRegularFile(artifact), "shaded plugin artifact is missing");

        try (JarFile jar = new JarFile(artifact.toFile())) {
            assertEntry(jar, "io/pyroscope/javaagent/PyroscopeAgent.class");
            assertEntry(jar, "io/pyroscope/javaagent/config/Config$Builder.class");
            assertEntry(jar, "io/pyroscope/vendor/one/profiler/AsyncProfiler.class");
            assertEntry(jar, "io/pyroscope/vendor/okhttp3/OkHttpClient.class");
            assertEntry(jar, "jfr/pyroscope.jfc");
            assertEntry(jar, "pyroscope-bootstrap.jar.bin");

            for (String library : NATIVE_LIBRARIES) {
                assertEntry(jar, library);
                assertEntry(jar, library + ".sha1");
                String expectedChecksum = text(jar, library + ".sha1").trim();
                assertTrue(expectedChecksum.matches("[0-9a-f]{40}"),
                        library + " checksum resource is invalid");
                assertEquals(expectedChecksum, sha1(bytes(jar, library)),
                        library + " does not match its embedded checksum");
            }

            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                assertFalse(entries.nextElement().getName()
                                .startsWith("cn/wangz/spark/telemetry/shaded/io/pyroscope/"),
                        "Pyroscope and its JNI classes must never be relocated");
            }

            String expectedVersion = System.getProperty("spark.telemetry.pyroscope.version");
            Attributes manifest = jar.getManifest().getMainAttributes();
            assertEquals(expectedVersion, manifest.getValue("Pyroscope-Agent-Version"));
            assertNull(manifest.getValue("Premain-Class"),
                    "the Spark plugin JAR must only use programmatic Pyroscope startup");
        }
    }

    @Test
    void packagedArtifactStartsAndStopsItsOwnNativeAsyncProfiler() throws Exception {
        String os = System.getProperty("os.name", "");
        String arch = System.getProperty("os.arch", "");
        boolean supportedOs = "Linux".equals(os) || "Mac OS X".equals(os);
        boolean supportedArch = "amd64".equals(arch) || "x86_64".equals(arch) ||
                "aarch64".equals(arch);
        Assumptions.assumeTrue(supportedOs && supportedArch,
                "Pyroscope's embedded async-profiler supports Linux/macOS on x64/arm64");

        URL artifact = pluginArtifact().toUri().toURL();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {artifact}, null)) {
            Class<?> bridge = Class.forName("io.pyroscope.PyroscopeAsyncProfiler", true, loader);
            Object profiler = bridge.getMethod("getAsyncProfiler").invoke(null);
            Method execute = profiler.getClass().getMethod("execute", String.class);
            String version = String.valueOf(execute.invoke(profiler, "version"));
            assertTrue(version.matches("\\d+(\\.\\d+)+"), version);

            Class<?> builderType = Class.forName(
                    "io.pyroscope.javaagent.config.Config$Builder", true, loader);
            Class<?> configType = Class.forName(
                    "io.pyroscope.javaagent.config.Config", true, loader);
            Class<?> eventType = Class.forName(
                    "io.pyroscope.javaagent.EventType", true, loader);
            Class<?> profilerType = Class.forName(
                    "io.pyroscope.javaagent.config.ProfilerType", true, loader);
            Class<?> formatType = Class.forName("io.pyroscope.http.Format", true, loader);
            Object builder = builderType.getConstructor().newInstance();
            builderType.getMethod("setApplicationName", String.class)
                    .invoke(builder, "shaded-artifact-smoke");
            builderType.getMethod("setServerAddress", String.class)
                    .invoke(builder, "http://127.0.0.1:1");
            builderType.getMethod("setProfilingEvent", eventType)
                    .invoke(builder, enumValue(eventType, "ITIMER"));
            builderType.getMethod("setProfilerType", profilerType)
                    .invoke(builder, enumValue(profilerType, "ASYNC"));
            builderType.getMethod("setFormat", formatType)
                    .invoke(builder, enumValue(formatType, "JFR"));
            builderType.getMethod("setProfilingInterval", Duration.class)
                    .invoke(builder, Duration.ofMillis(20));
            builderType.getMethod("setUploadInterval", Duration.class)
                    .invoke(builder, Duration.ofSeconds(2));
            builderType.getMethod("setLabels", java.util.Map.class)
                    .invoke(builder, Collections.emptyMap());
            builderType.getMethod("setAPExtraArguments", String.class)
                    .invoke(builder, "cstack=vmx,memlimit=32m");
            Object config = builderType.getMethod("build").invoke(builder);

            Class<?> agent = Class.forName("io.pyroscope.javaagent.PyroscopeAgent", true, loader);
            Method isStarted = agent.getMethod("isStarted");
            try {
                agent.getMethod("start", configType).invoke(null, config);
                assertEquals(Boolean.TRUE, isStarted.invoke(null));
            } finally {
                agent.getMethod("stop").invoke(null);
            }
            assertEquals(Boolean.FALSE, isStarted.invoke(null));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object enumValue(Class<?> type, String name) {
        return Enum.valueOf((Class<? extends Enum>) type.asSubclass(Enum.class), name);
    }

    private static Path pluginArtifact() {
        String configured = System.getProperty("spark.telemetry.plugin.jar");
        assertNotNull(configured, "spark.telemetry.plugin.jar is not configured by Maven");
        return Paths.get(configured);
    }

    private static void assertEntry(JarFile jar, String name) {
        JarEntry entry = jar.getJarEntry(name);
        assertNotNull(entry, "missing shaded entry " + name);
        assertTrue(entry.getSize() != 0L, "empty shaded entry " + name);
    }

    private static String text(JarFile jar, String name) throws IOException {
        return new String(bytes(jar, name), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(JarFile jar, String name) throws IOException {
        JarEntry entry = jar.getJarEntry(name);
        assertNotNull(entry, "missing shaded entry " + name);
        try (InputStream input = jar.getInputStream(entry);
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) output.write(buffer, 0, read);
            return output.toByteArray();
        }
    }

    private static String sha1(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(data);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) result.append(String.format("%02x", value & 0xff));
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-1 is required by the Java platform", impossible);
        }
    }
}
