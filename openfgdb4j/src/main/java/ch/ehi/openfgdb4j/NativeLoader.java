package ch.ehi.openfgdb4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class NativeLoader {
    private static volatile boolean loaded;
    private static final Object IN_PROCESS_EXTRACTION_LOCK = new Object();

    private NativeLoader() {
    }

    public static synchronized void load() {
        if (loaded) {
            return;
        }
        Platform platform = ensureSupportedPlatform();

        List<File> candidates = new ArrayList<File>();
        List<String> resourceCandidates = resourceCandidates(platform);

        String overridePath = System.getProperty("openfgdb4j.lib");
        if (overridePath != null && !overridePath.isEmpty()) {
            candidates.add(new File(overridePath));
        }
        String overrideEnv = System.getenv("OPENFGDB4J_LIB");
        if (overrideEnv != null && !overrideEnv.isEmpty()) {
            candidates.add(new File(overrideEnv));
        }

        File fromResources = resolveEmbeddedCandidate(platform, resourceCandidates);
        if (fromResources != null) {
            candidates.add(fromResources);
        }

        addBuildCandidates(candidates, platform.libraryNames);
        candidates.addAll(resolveCodeSourceCandidates(platform.libraryNames));

        File selected = null;
        for (File candidate : candidates) {
            if (candidate.exists() && candidate.isFile()) {
                selected = candidate;
                break;
            }
        }

        if (selected == null) {
            throw new IllegalStateException(
                    "Native library not found for os="
                            + platform.os
                            + ", arch="
                            + platform.arch
                            + ", resourceCandidates="
                            + resourceCandidates
                            + ". Looked in: "
                            + candidates);
        }

        System.load(selected.getAbsolutePath());
        loaded = true;
    }

    private static File resolveEmbeddedCandidate(Platform platform, List<String> resourceCandidates) {
        return resolveEmbeddedCandidate(platform, resourceCandidates, NativeLoader.class.getClassLoader());
    }

    static File resolveEmbeddedCandidate(Platform platform, List<String> resourceCandidates, ClassLoader classLoader) {
        for (String resourcePath : resourceCandidates) {
            try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
                if (input == null) {
                    continue;
                }
                String libName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
                return extractToCache(input, platform, libName);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read native resource " + resourcePath, e);
            }
        }
        return null;
    }

    private static File extractToCache(InputStream input, Platform platform, String libName) {
        Path cacheRoot = resolveCacheRoot();
        Path platformDir = cacheRoot.resolve(platform.id);
        try {
            Files.createDirectories(platformDir);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create native cache directory " + platformDir, e);
        }

        Path lockFile = platformDir.resolve(".native-loader.lock");
        synchronized (IN_PROCESS_EXTRACTION_LOCK) {
            try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    FileLock ignored = channel.lock()) {
                return extractToCacheLocked(input, platformDir, libName).toFile();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to lock native cache directory " + platformDir, e);
            }
        }
    }

    private static Path extractToCacheLocked(InputStream input, Path platformDir, String libName) {
        MessageDigest digest = sha256();
        String ext = extension(libName);
        String stem = stem(libName, ext);
        String versionTag = sanitizeToken(resolveVersionTag());

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(platformDir, stem + "-", ext + ".tmp");
            long bytesCopied = copyWithDigest(input, tempFile, digest);
            if (bytesCopied <= 0) {
                throw new IllegalStateException("Native resource " + libName + " is empty");
            }

            String hash = toHex(digest.digest());
            String targetName = stem + "-" + versionTag + "-" + hash + ext;
            Path targetFile = platformDir.resolve(targetName);
            if (Files.exists(targetFile)) {
                Files.deleteIfExists(tempFile);
                return targetFile;
            }

            moveAtomically(tempFile, targetFile);
            return targetFile;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to extract native resource " + libName + " to " + platformDir, e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignore) {
                }
            }
        }
    }

    private static Path resolveCacheRoot() {
        String fromProp = System.getProperty("openfgdb4j.cache.dir");
        if (fromProp != null && !fromProp.isEmpty()) {
            return Paths.get(fromProp);
        }
        String fromEnv = System.getenv("OPENFGDB4J_CACHE_DIR");
        if (fromEnv != null && !fromEnv.isEmpty()) {
            return Paths.get(fromEnv);
        }

        String userHome = System.getProperty("user.home", "");
        if (!userHome.isEmpty()) {
            return Paths.get(userHome, ".openfgdb4j", "cache");
        }

        return Paths.get(System.getProperty("java.io.tmpdir"), "openfgdb4j", "cache");
    }

    private static long copyWithDigest(InputStream input, Path target, MessageDigest digest) throws IOException {
        long total = 0L;
        byte[] buffer = new byte[8192];
        try (java.io.OutputStream output = Files.newOutputStream(target, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (true) {
                int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                output.write(buffer, 0, read);
                digest.update(buffer, 0, read);
                total += read;
            }
        }
        return total;
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            return;
        } catch (FileAlreadyExistsException e) {
            return;
        } catch (AtomicMoveNotSupportedException e) {
            // Fall through and use non-atomic move below.
        }

        try {
            Files.move(source, target);
        } catch (FileAlreadyExistsException e) {
            // Another process wrote the same file concurrently.
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    private static String resolveVersionTag() {
        Package pkg = NativeLoader.class.getPackage();
        if (pkg != null) {
            String impl = pkg.getImplementationVersion();
            if (impl != null && !impl.isEmpty()) {
                return impl;
            }
        }
        String prop = System.getProperty("openfgdb4j.version");
        if (prop != null && !prop.isEmpty()) {
            return prop;
        }
        String env = System.getenv("OPENFGDB4J_VERSION");
        if (env != null && !env.isEmpty()) {
            return env;
        }
        return "dev";
    }

    static String sanitizeToken(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "dev";
        }
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '.'
                    || c == '_'
                    || c == '-') {
                out.append(c);
            } else {
                out.append('_');
            }
        }
        return out.toString();
    }

    private static String toHex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            int v = value & 0xff;
            if (v < 16) {
                out.append('0');
            }
            out.append(Integer.toHexString(v));
        }
        return out.toString();
    }

    private static String extension(String fileName) {
        int idx = fileName.lastIndexOf('.');
        if (idx < 0) {
            return "";
        }
        return fileName.substring(idx);
    }

    private static String stem(String fileName, String ext) {
        if (ext.isEmpty()) {
            return fileName;
        }
        return fileName.substring(0, fileName.length() - ext.length());
    }

    private static void addBuildCandidates(List<File> candidates, List<String> libraryNames) {
        for (String libName : libraryNames) {
            candidates.add(new File("openfgdb4j/build/native/" + libName));
            candidates.add(new File("openfgdb4j/build/native/Release/" + libName));
            candidates.add(new File("build/native/" + libName));
            candidates.add(new File("build/native/Release/" + libName));
            candidates.add(new File("openfgdb4j/build/native/Debug/" + libName));
            candidates.add(new File("build/native/Debug/" + libName));
        }
    }

    static Platform ensureSupportedPlatform() {
        String normalizedOs = normalizeOs(System.getProperty("os.name", ""));
        String normalizedArch = normalizeArch(System.getProperty("os.arch", ""));

        boolean supported =
                ("linux".equals(normalizedOs) && ("amd64".equals(normalizedArch) || "arm64".equals(normalizedArch)))
                        || ("macos".equals(normalizedOs)
                                && ("amd64".equals(normalizedArch) || "arm64".equals(normalizedArch)))
                        || ("windows".equals(normalizedOs) && "amd64".equals(normalizedArch));

        if (!supported) {
            throw new IllegalStateException(
                    "openfgdb4j supports linux(amd64,arm64), macos(amd64,arm64), windows(amd64) "
                            + "(detected os="
                            + normalizedOs
                            + ", arch="
                            + normalizedArch
                            + ")");
        }

        String ext = detectExtension(normalizedOs);
        List<String> names = new ArrayList<String>();
        if ("windows".equals(normalizedOs)) {
            names.add("openfgdb" + ext);
            names.add("libopenfgdb" + ext);
        } else {
            names.add("libopenfgdb" + ext);
        }
        String platformId = normalizedOs + "-" + normalizedArch;
        return new Platform(normalizedOs, normalizedArch, platformId, names);
    }

    static List<String> resourceCandidates(Platform platform) {
        List<String> resources = new ArrayList<String>();
        for (String libName : platform.libraryNames) {
            resources.add("META-INF/openfgdb4j/native/" + platform.id + "/" + libName);
        }
        return resources;
    }

    static String normalizeOs(String rawOs) {
        String os = rawOs.toLowerCase();
        if (os.contains("win")) {
            return "windows";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "macos";
        }
        if (os.contains("nux") || os.contains("linux")) {
            return "linux";
        }
        return os;
    }

    static String normalizeArch(String rawArch) {
        String arch = rawArch.toLowerCase();
        if ("x86_64".equals(arch) || "amd64".equals(arch)) {
            return "amd64";
        }
        if ("aarch64".equals(arch) || "arm64".equals(arch)) {
            return "arm64";
        }
        return arch;
    }

    static String detectExtension(String os) {
        if ("windows".equals(os)) {
            return ".dll";
        }
        if ("macos".equals(os)) {
            return ".dylib";
        }
        return ".so";
    }

    private static List<File> resolveCodeSourceCandidates(List<String> libraryNames) {
        try {
            URI uri = NativeLoader.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            File codeSource = new File(uri);
            File baseDir = codeSource.isDirectory() ? codeSource : codeSource.getParentFile();
            if (baseDir == null) {
                return Collections.emptyList();
            }
            List<File> candidates = new ArrayList<File>();
            for (String libName : libraryNames) {
                candidates.add(new File(baseDir, libName));
                candidates.add(new File(baseDir, "native/" + libName));
                candidates.add(new File(baseDir, "../native/" + libName));
                candidates.add(new File(baseDir, "../libs/" + libName));
                candidates.add(new File(baseDir, "../openfgdb4j/build/native/" + libName));
            }
            return candidates;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    static final class Platform {
        final String os;
        final String arch;
        final String id;
        final List<String> libraryNames;

        private Platform(String os, String arch, String id, List<String> libraryNames) {
            this.os = os;
            this.arch = arch;
            this.id = id;
            this.libraryNames = libraryNames;
        }
    }
}
