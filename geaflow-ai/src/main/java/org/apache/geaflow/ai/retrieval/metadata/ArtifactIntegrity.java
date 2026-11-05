/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.retrieval.metadata;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/** Shared file and READY-manifest integrity checks for GraphRAG artifacts. */
public final class ArtifactIntegrity {

    private static final int DIGEST_BUFFER_SIZE = 1024 * 1024;

    private ArtifactIntegrity() {
    }

    /** Returns the lowercase SHA-256 digest of a file, reading it in bounded memory. */
    public static String sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IOException("SHA-256 is unavailable", error);
        }
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[DIGEST_BUFFER_SIZE];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, count);
            }
        }
        return hex(digest.digest());
    }

    /** Returns the lowercase SHA-256 digest of an in-memory value. */
    public static String sha256(byte[] value) throws IOException {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException error) {
            throw new IOException("SHA-256 is unavailable", error);
        }
    }

    /** Resolves a manifest-relative artifact path and rejects traversal and external symlinks. */
    public static Path resolveInside(Path root, String relative) throws IOException {
        if (root == null || relative == null || relative.trim().isEmpty()) {
            throw new IOException("artifact root and relative path are required");
        }
        String portableRelative = relative.replace('\\', '/');
        Path relativePath = Path.of(portableRelative);
        Path absoluteRoot = root.toAbsolutePath().normalize();
        Path candidate = absoluteRoot.resolve(relativePath).normalize();
        if (relativePath.isAbsolute() || !candidate.startsWith(absoluteRoot)
            || !Files.exists(candidate)) {
            throw new IOException("artifact path escapes its root or is missing: " + relative);
        }
        Path realRoot = root.toRealPath();
        Path realCandidate = candidate.toRealPath();
        if (!realCandidate.startsWith(realRoot)) {
            throw new IOException("artifact path escapes its root: " + relative);
        }
        return candidate;
    }

    /**
     * Verifies every declared checksum and requires the manifest to cover every release file
     * except READY.json. Directory entries are intentionally ignored.
     */
    public static void validateChecksums(Path root, JsonObject checksums) throws IOException {
        if (root == null || !Files.isDirectory(root)) {
            throw new IOException("artifact bundle directory is missing: " + root);
        }
        if (checksums == null || checksums.entrySet().isEmpty()) {
            throw new IOException("artifact checksum manifest is missing or empty");
        }
        Path absoluteRoot = root.toAbsolutePath().normalize();
        Set<String> expectedPaths = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : checksums.entrySet()) {
            String relative = entry.getKey().replace('\\', '/');
            Path file = resolveInside(absoluteRoot, relative);
            if (!Files.isRegularFile(file)) {
                throw new IOException("artifact checksum target is not a file: " + entry.getKey());
            }
            if (entry.getValue() == null || !entry.getValue().isJsonPrimitive()) {
                throw new IOException("artifact checksum is not a SHA-256 digest: " + entry.getKey());
            }
            String expected = entry.getValue().getAsString();
            if (!expected.matches("[0-9a-fA-F]{64}")) {
                throw new IOException("artifact checksum is not a SHA-256 digest: " + entry.getKey());
            }
            if (!expectedPaths.add(relative)) {
                throw new IOException("artifact checksum path is declared more than once: " + relative);
            }
            if (!sha256(file).equalsIgnoreCase(expected)) {
                throw new IOException("artifact checksum mismatch: " + entry.getKey());
            }
        }

        Set<String> actualPaths = new HashSet<>();
        Path absoluteReady = absoluteRoot.resolve("READY.json");
        try (Stream<Path> paths = Files.walk(absoluteRoot)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                if (path.equals(absoluteReady)) {
                    continue;
                }
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("artifact bundle contains a symbolic link: " + path);
                }
                if (Files.isRegularFile(path)) {
                    actualPaths.add(toManifestPath(absoluteRoot.relativize(path)));
                }
            }
        }
        if (!actualPaths.equals(expectedPaths)) {
            throw new IOException("artifact checksum manifest does not cover bundle files exactly");
        }
    }

    /** Deletes a tree in reverse path order without following symbolic links. */
    public static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        List<Path> entries = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(entries::add);
        }
        for (Path entry : entries) {
            Files.deleteIfExists(entry);
        }
    }

    private static String toManifestPath(Path relative) {
        return relative.toString().replace(relative.getFileSystem().getSeparator(), "/");
    }

    private static String hex(byte[] digest) {
        StringBuilder result = new StringBuilder();
        for (byte value : digest) {
            result.append(String.format(java.util.Locale.ROOT, "%02x", value & 0xff));
        }
        return result.toString();
    }
}
