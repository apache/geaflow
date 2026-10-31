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
package org.apache.geaflow.ai.retrieval.index.vector;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.index.ChannelSearchResult;

/**
 * Reader for the binary format emitted by {@link OfflineVectorIndexBuilder}.
 */
public final class VectorIndexReader implements Closeable {
    private static final String MAGIC = "GEAFLOW-VECTOR-2";
    private final ArtifactIdentity identity;
    private final List<Entry> entries;
    private final int dimensions;
    private final String vectorSource;
    private final String vectorVersion;

    public VectorIndexReader(Path artifact) {
        this(artifact, null, null, null, null);
    }

    public VectorIndexReader(Path artifact, String expectedGraphVersion, String expectedIndexVersion,
                             String expectedSource, String expectedVectorVersion) {
        if (artifact == null || !Files.isRegularFile(artifact)) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                "missing vector artifact: " + artifact);
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(artifact))) {
            if (!MAGIC.equals(input.readUTF())) {
                throw new IOException("invalid vector artifact magic");
            }
            identity = new ArtifactIdentity(input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF());
            identity.validateVersions(expectedGraphVersion, expectedIndexVersion);
            vectorSource = input.readUTF();
            vectorVersion = input.readUTF();
            if (expectedSource != null && !expectedSource.equals(vectorSource)
                || expectedVectorVersion != null && !expectedVectorVersion.equals(vectorVersion)) {
                throw new IOException("vector source/version mismatch");
            }
            dimensions = input.readInt();
            int count = input.readInt();
            if (dimensions <= 0 || count < 0 || (long) dimensions * 4 > Files.size(artifact)
                || (long) count * ((long) dimensions * 4 + 4) > Files.size(artifact)) {
                throw new IOException("invalid vector dimensions/count");
            }
            entries = new ArrayList<>(count);
            Set<String> chunkIds = new HashSet<>();
            for (int i = 0; i < count; i++) {
                String chunkId = input.readUTF();
                String documentId = input.readUTF();
                if (chunkId.isEmpty() || documentId.isEmpty() || !chunkIds.add(chunkId)) {
                    throw new IOException("invalid or duplicate vector chunk ID");
                }
                float[] vector = new float[dimensions];
                for (int d = 0; d < dimensions; d++) {
                    vector[d] = input.readFloat();
                    if (!Float.isFinite(vector[d])) {
                        throw new IOException("non-finite vector value");
                    }
                }
                if (norm(vector) == 0.0) {
                    throw new IOException("zero-norm artifact vector");
                }
                entries.add(new Entry(chunkId, documentId, vector));
            }
            entries.sort(Comparator.comparing((Entry entry) -> entry.chunkId));
            if (input.read() != -1) {
                throw new IOException("trailing vector artifact data");
            }
        } catch (Exception error) {
            if (error instanceof RetrievalException) {
                throw (RetrievalException) error;
            }
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                "unable to read vector artifact", error);
        }
    }

    public ArtifactIdentity getIdentity() {
        return identity;
    }

    public int getDimensions() {
        return dimensions;
    }

    public String getVectorSource() {
        return vectorSource;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public List<VectorHit> search(float[] query, int candidateLimit, int topK) {
        return search(query, candidateLimit, topK, 0L);
    }

    public List<VectorHit> search(float[] query, int candidateLimit, int topK, long deadlineNanos) {
        List<VectorHit> hits = searchWithStats(query, candidateLimit, topK, deadlineNanos).getHits();
        return new ArrayList<>(hits.subList(0, Math.min(topK, hits.size())));
    }

    public ChannelSearchResult<VectorHit> searchWithStats(float[] query, int candidateLimit,
                                                          int topK, long deadlineNanos) {
        return searchWithStats(query, candidateLimit, topK, deadlineNanos, System::nanoTime);
    }

    public ChannelSearchResult<VectorHit> searchWithStats(float[] query, int candidateLimit,
                                                          int topK, long deadlineNanos, LongSupplier clock) {
        validateQuery(query);
        if (candidateLimit < 1 || topK < 1) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid vector budget");
        }
        double queryNorm = norm(query);
        List<VectorHit> hits = new ArrayList<>();
        int evaluated = 0;
        RecallStopReason reason = RecallStopReason.COMPLETED;
        for (Entry entry : entries) {
            if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
                reason = RecallStopReason.DEADLINE;
                break;
            }
            if (evaluated >= candidateLimit) {
                reason = RecallStopReason.CANDIDATE_BUDGET;
                break;
            }
            evaluated++;
            double score = dot(query, entry.vector) / (queryNorm * norm(entry.vector));
            if (Double.isFinite(score)) {
                hits.add(new VectorHit(entry.chunkId, entry.documentId, score, 0));
            }
        }
        hits.sort(Comparator.comparing(VectorHit::getSimilarity).reversed()
            .thenComparing(VectorHit::getChunkId));
        List<VectorHit> result = new ArrayList<>();
        int rank = 1;
        for (VectorHit hit : hits) {
            if (rank > candidateLimit) {
                break;
            }
            result.add(new VectorHit(hit.getChunkId(), hit.getDocumentId(), hit.getSimilarity(), rank++));
        }
        if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
            reason = RecallStopReason.DEADLINE;
        }
        return new ChannelSearchResult<>(result, evaluated, reason);
    }

    public void validateQuery(float[] query) {
        validateValues(query);
        if (query.length != dimensions) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "query vector dimensions do not match artifact");
        }
    }

    public static void validateValues(float[] query) {
        if (query == null || query.length == 0) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "queryVector is required");
        }
        for (float value : query) {
            if (!Float.isFinite(value)) {
                throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "query vector must contain finite values");
            }
        }
        double queryNorm = norm(query);
        if (!Double.isFinite(queryNorm) || queryNorm == 0.0) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST,
                "query vector must have a finite, non-zero norm");
        }
    }

    private static double dot(float[] first, float[] second) {
        double value = 0.0;
        for (int i = 0; i < first.length; i++) {
            value += (double) first[i] * second[i];
        }
        return value;
    }

    private static double norm(float[] vector) {
        return Math.sqrt(dot(vector, vector));
    }

    @Override
    public void close() {
        // The artifact is loaded into immutable memory; no resources remain open.
    }

    private static final class Entry {
        private final String chunkId;
        private final String documentId;
        private final float[] vector;

        private Entry(String chunkId, String documentId, float[] vector) {
            this.chunkId = chunkId;
            this.documentId = documentId;
            this.vector = vector;
        }
    }
}
