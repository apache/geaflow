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

import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArtifactIntegrityTest {

    @TempDir
    private Path temporaryDirectory;

    @Test
    void validatesExactChecksumCoverageAndAllowsReadyMetadata() throws Exception {
        Path release = Files.createDirectory(temporaryDirectory.resolve("release"));
        Path artifact = Files.createDirectories(release.resolve("graph")).resolve("edges.jsonl");
        Files.writeString(artifact, "edge\n", StandardCharsets.UTF_8);
        Files.writeString(release.resolve("READY.json"), "{}\n", StandardCharsets.UTF_8);

        JsonObject checksums = checksums(release, "graph/edges.jsonl");
        assertDoesNotThrow(() -> ArtifactIntegrity.validateChecksums(release, checksums));

        Files.writeString(release.resolve("untracked.bin"), "extra", StandardCharsets.UTF_8);
        assertThrows(IOException.class,
            () -> ArtifactIntegrity.validateChecksums(release, checksums));
    }

    @Test
    void rejectsMissingFilesAndDigestMismatches() throws Exception {
        Path release = Files.createDirectory(temporaryDirectory.resolve("release"));
        Files.writeString(release.resolve("READY.json"), "{}\n", StandardCharsets.UTF_8);
        JsonObject missing = new JsonObject();
        missing.addProperty("missing.bin", "0".repeat(64));
        assertThrows(IOException.class,
            () -> ArtifactIntegrity.validateChecksums(release, missing));

        Path artifact = release.resolve("artifact.bin");
        Files.writeString(artifact, "contents", StandardCharsets.UTF_8);
        JsonObject mismatch = new JsonObject();
        mismatch.addProperty("artifact.bin", "0".repeat(64));
        assertThrows(IOException.class,
            () -> ArtifactIntegrity.validateChecksums(release, mismatch));
    }

    @Test
    void rejectsTraversalAndSymlinkTargetsOutsideTheBundle() throws Exception {
        Path root = Files.createDirectory(temporaryDirectory.resolve("bundle"));
        Path outside = temporaryDirectory.resolve("outside.bin");
        Files.writeString(outside, "outside", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> ArtifactIntegrity.resolveInside(root, "../outside.bin"));

        Path link = root.resolve("outside.bin");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | IOException | SecurityException unavailable) {
            Assumptions.abort("symbolic links are unavailable in this environment");
        }
        JsonObject checksums = new JsonObject();
        checksums.addProperty("outside.bin", ArtifactIntegrity.sha256(outside));
        assertThrows(IOException.class, () -> ArtifactIntegrity.validateChecksums(root, checksums));
    }

    private static JsonObject checksums(Path root, String... relativePaths) throws IOException {
        JsonObject checksums = new JsonObject();
        for (String relative : relativePaths) {
            checksums.addProperty(relative, ArtifactIntegrity.sha256(root.resolve(relative)));
        }
        return checksums;
    }
}
