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
package org.apache.geaflow.ai.retrieval.index;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/** Persisted graph/index identity and canonical chunk fingerprint shared by both indexes. */
public final class ArtifactIdentity {
    private static final String SAFE_VERSION = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";
    private final String graphName;
    private final String graphVersion;
    private final String indexVersion;
    private final String chunkFingerprint;

    public ArtifactIdentity(String graphName, String graphVersion, String indexVersion, String chunkFingerprint) {
        if (blank(graphName) || blank(graphVersion) || blank(indexVersion) || !graphVersion.equals(indexVersion)
            || chunkFingerprint == null || !chunkFingerprint.matches("[0-9a-f]{64}")) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "artifact identity missing; rebuild index");
        }
        this.graphName = graphName;
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.chunkFingerprint = chunkFingerprint;
    }

    public static ArtifactIdentity fromMap(Map<String, String> metadata) {
        return new ArtifactIdentity(metadata.get("graphName"), metadata.get("graphVersion"),
            metadata.get("indexVersion"), metadata.get("chunkFingerprint"));
    }

    /** Resolves an artifact target while keeping the version inside its output directory. */
    public static Path resolveVersionedPath(Path outputDirectory, String prefix, String version, String suffix)
        throws IOException {
        if (version == null || !version.matches(SAFE_VERSION)) {
            throw new IOException("invalid artifact version for output path");
        }
        Path directory = outputDirectory.toAbsolutePath().normalize();
        Path target = directory.resolve(prefix + version + suffix).normalize();
        if (!directory.equals(target.getParent())) {
            throw new IOException("artifact path escapes output directory");
        }
        return target;
    }

    public Map<String, String> toMap() {
        Map<String, String> metadata = new TreeMap<>();
        metadata.put("graphName", graphName);
        metadata.put("graphVersion", graphVersion);
        metadata.put("indexVersion", indexVersion);
        metadata.put("chunkFingerprint", chunkFingerprint);
        return metadata;
    }

    public void validate(String name, String graph, String index, List<TextChunk> chunks) {
        if (!graphName.equals(name) || !graphVersion.equals(graph) || !indexVersion.equals(index)
            || !chunkFingerprint.equals(fingerprint(chunks))) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "artifact/fixture identity mismatch");
        }
    }

    public void validateVersions(String graph, String index) {
        if ((graph != null && !graphVersion.equals(graph)) || (index != null && !indexVersion.equals(index))) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "artifact version mismatch");
        }
    }

    public static String fingerprint(List<TextChunk> chunks) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            List<TextChunk> ordered = new ArrayList<>(chunks);
            ordered.sort(Comparator.comparing(TextChunk::getDocumentId).thenComparing(TextChunk::getChunkId));
            for (TextChunk chunk : ordered) {
                append(digest, chunk.getDocumentId());
                append(digest, chunk.getChunkId());
                append(digest, chunk.getText());
                append(digest, Integer.toString(chunk.getChunkIndex()));
                append(digest, Integer.toString(chunk.getTokenEstimate()));
                append(digest, chunk.getPolicyVersion());
                append(digest, chunk.getTextHash());
                append(digest, chunk.getSourceUri());
                append(digest, Integer.toString(chunk.getStartOffset()));
                append(digest, Integer.toString(chunk.getEndOffset()));
            }
            StringBuilder result = new StringBuilder();
            for (byte value : digest.digest()) {
                result.append(String.format(java.util.Locale.ROOT, "%02x", value & 255));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 unavailable", error);
        }
    }

    private static void append(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(java.nio.ByteBuffer.allocate(4).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
