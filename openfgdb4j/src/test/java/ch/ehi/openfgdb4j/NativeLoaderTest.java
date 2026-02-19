package ch.ehi.openfgdb4j;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.Assume;
import org.junit.Test;

public class NativeLoaderTest {

    @Test
    public void normalizeOsAndArch() {
        assertEquals("windows", NativeLoader.normalizeOs("Windows 11"));
        assertEquals("linux", NativeLoader.normalizeOs("linux"));
        assertEquals("macos", NativeLoader.normalizeOs("Mac OS X"));

        assertEquals("amd64", NativeLoader.normalizeArch("x86_64"));
        assertEquals("amd64", NativeLoader.normalizeArch("amd64"));
        assertEquals("arm64", NativeLoader.normalizeArch("aarch64"));
        assertEquals("arm64", NativeLoader.normalizeArch("arm64"));

        assertEquals(".dll", NativeLoader.detectExtension("windows"));
        assertEquals(".dylib", NativeLoader.detectExtension("macos"));
        assertEquals(".so", NativeLoader.detectExtension("linux"));
    }

    @Test
    public void resourceCandidatesContainPlatformPrefix() {
        NativeLoader.Platform platform = supportedPlatformOrSkip();
        List<String> resources = NativeLoader.resourceCandidates(platform);
        assertFalse(resources.isEmpty());
        for (String resource : resources) {
            assertTrue(resource.startsWith("META-INF/openfgdb4j/native/" + platform.id + "/"));
        }
    }

    @Test
    public void resolveEmbeddedCandidateExtractsFromJarResource() throws Exception {
        NativeLoader.Platform platform = supportedPlatformOrSkip();
        String resourceName = "META-INF/openfgdb4j/native/" + platform.id + "/" + platform.libraryNames.get(0);
        byte[] payload = new byte[] {1, 2, 3, 4, 5, 6};

        Path tempDir = Files.createTempDirectory("native-loader-resource-");
        Path cacheDir = tempDir.resolve("cache");
        Path jarPath = tempDir.resolve("natives.jar");
        writeJarWithEntry(jarPath, resourceName, payload);

        String oldCacheProp = System.getProperty("openfgdb4j.cache.dir");
        System.setProperty("openfgdb4j.cache.dir", cacheDir.toString());
        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {jarPath.toUri().toURL()}, null)) {
            File extracted = NativeLoader.resolveEmbeddedCandidate(platform, Arrays.asList(resourceName), classLoader);
            assertNotNull(extracted);
            assertTrue(extracted.exists());
            assertTrue(extracted.getName().contains("-"));
            assertArrayEquals(payload, Files.readAllBytes(extracted.toPath()));
        } finally {
            restoreCacheDirProperty(oldCacheProp);
        }
    }

    @Test
    public void concurrentExtractionReturnsSameTargetFile() throws Exception {
        NativeLoader.Platform platform = supportedPlatformOrSkip();
        String resourceName = "META-INF/openfgdb4j/native/" + platform.id + "/" + platform.libraryNames.get(0);
        byte[] payload = new byte[] {9, 8, 7, 6, 5, 4, 3};

        Path tempDir = Files.createTempDirectory("native-loader-concurrent-");
        Path cacheDir = tempDir.resolve("cache");
        Path jarPath = tempDir.resolve("natives.jar");
        writeJarWithEntry(jarPath, resourceName, payload);

        String oldCacheProp = System.getProperty("openfgdb4j.cache.dir");
        System.setProperty("openfgdb4j.cache.dir", cacheDir.toString());
        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {jarPath.toUri().toURL()}, null)) {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Callable<File> loaderCall = () -> NativeLoader.resolveEmbeddedCandidate(platform, Arrays.asList(resourceName), classLoader);
                Future<File> f1 = executor.submit(loaderCall);
                Future<File> f2 = executor.submit(loaderCall);
                File a = f1.get();
                File b = f2.get();
                assertNotNull(a);
                assertNotNull(b);
                assertEquals(a.getAbsolutePath(), b.getAbsolutePath());
                assertArrayEquals(payload, Files.readAllBytes(a.toPath()));
            } finally {
                executor.shutdownNow();
            }
        } finally {
            restoreCacheDirProperty(oldCacheProp);
        }
    }

    @Test
    public void concurrentExtractionAcrossJvmsReturnsSameTargetFile() throws Exception {
        NativeLoader.Platform platform = supportedPlatformOrSkip();
        String resourceName = "META-INF/openfgdb4j/native/" + platform.id + "/" + platform.libraryNames.get(0);
        byte[] payload = new byte[] {4, 5, 6, 7, 8, 9};

        Path tempDir = Files.createTempDirectory("native-loader-process-");
        Path cacheDir = tempDir.resolve("cache");
        Path jarPath = tempDir.resolve("natives.jar");
        writeJarWithEntry(jarPath, resourceName, payload);

        Process p1 = startExtractionProcess(cacheDir, jarPath, resourceName);
        Process p2 = startExtractionProcess(cacheDir, jarPath, resourceName);

        int exit1 = p1.waitFor();
        int exit2 = p2.waitFor();

        String out1 = new String(p1.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        String out2 = new String(p2.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        String err1 = new String(p1.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        String err2 = new String(p2.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).trim();

        assertEquals("First extraction process failed: " + err1, 0, exit1);
        assertEquals("Second extraction process failed: " + err2, 0, exit2);
        assertFalse("First process did not report extraction path", out1.isEmpty());
        assertFalse("Second process did not report extraction path", out2.isEmpty());
        assertEquals(out1, out2);
        assertArrayEquals(payload, Files.readAllBytes(Path.of(out1)));
    }

    private static NativeLoader.Platform supportedPlatformOrSkip() {
        try {
            return NativeLoader.ensureSupportedPlatform();
        } catch (IllegalStateException e) {
            Assume.assumeTrue("Unsupported runtime for NativeLoader tests: " + e.getMessage(), false);
            return null;
        }
    }

    private static void writeJarWithEntry(Path jarPath, String entryName, byte[] bytes) throws IOException {
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath))) {
            JarEntry entry = new JarEntry(entryName);
            out.putNextEntry(entry);
            out.write(bytes);
            out.closeEntry();
        }
    }

    private static void restoreCacheDirProperty(String previous) {
        if (previous == null) {
            System.clearProperty("openfgdb4j.cache.dir");
        } else {
            System.setProperty("openfgdb4j.cache.dir", previous);
        }
    }

    private static Process startExtractionProcess(Path cacheDir, Path jarPath, String resourceName) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaExe = "java";
        if (javaHome != null && !javaHome.isEmpty()) {
            String suffix = NativeLoader.normalizeOs(System.getProperty("os.name", "")).equals("windows") ? ".exe" : "";
            javaExe = Path.of(javaHome, "bin", "java" + suffix).toString();
        }

        ProcessBuilder pb =
                new ProcessBuilder(
                        javaExe,
                        "-cp",
                        System.getProperty("java.class.path"),
                        "ch.ehi.openfgdb4j.NativeLoaderExtractMain",
                        cacheDir.toString(),
                        jarPath.toString(),
                        resourceName);
        return pb.start();
    }
}

final class NativeLoaderExtractMain {
    private NativeLoaderExtractMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: NativeLoaderExtractMain <cacheDir> <jarPath> <resource>");
        }
        Path cacheDir = Path.of(args[0]);
        Path jarPath = Path.of(args[1]);
        String resource = args[2];

        System.setProperty("openfgdb4j.cache.dir", cacheDir.toString());
        NativeLoader.Platform platform = NativeLoader.ensureSupportedPlatform();
        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {jarPath.toUri().toURL()}, null)) {
            File extracted = NativeLoader.resolveEmbeddedCandidate(platform, Arrays.asList(resource), classLoader);
            if (extracted == null) {
                throw new IllegalStateException("Native resource not found in jar: " + resource);
            }
            System.out.println(extracted.getAbsolutePath());
        }
    }
}
