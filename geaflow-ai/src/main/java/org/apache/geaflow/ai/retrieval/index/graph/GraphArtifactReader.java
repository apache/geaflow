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

package org.apache.geaflow.ai.retrieval.index.graph;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Reader for the deterministic GEAFLOW-GRAPH-1 binary artifact. */
public final class GraphArtifactReader implements Closeable {
    private static final String MAGIC = "GEAFLOW-GRAPH-1";
    private final ArtifactIdentity identity;
    private final List<EntityRef> entities;
    private final List<GraphEdgeRef> edges;
    private final GraphNeighborProvider neighbors;

    public GraphArtifactReader(Path artifact, String expectedGraphVersion, String expectedIndexVersion) {
        if (artifact == null || !Files.isRegularFile(artifact)) {
            throw notReady("missing graph artifact: " + artifact);
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(artifact))) {
            if (!MAGIC.equals(input.readUTF())) {
                throw new IOException("invalid graph artifact magic");
            }
            identity = new ArtifactIdentity(input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF());
            identity.validateVersions(expectedGraphVersion, expectedIndexVersion);
            int entityCount = input.readInt();
            int edgeCount = input.readInt();
            if (entityCount < 0 || edgeCount < 0) {
                throw new IOException("invalid graph record counts");
            }
            long minimumRecordBytes = (long) entityCount * 14L + (long) edgeCount * 12L;
            if (minimumRecordBytes > input.available()) {
                throw new IOException("graph record counts exceed artifact size");
            }
            entities = new ArrayList<>(entityCount);
            Set<String> entityIds = new HashSet<>();
            String previousEntityId = null;
            for (int index = 0; index < entityCount; index++) {
                String entityId = input.readUTF();
                final String canonical = input.readUTF();
                final String type = input.readUTF();
                int aliasCount = input.readInt();
                if (aliasCount < 0 || !entityIds.add(entityId)
                    || previousEntityId != null && previousEntityId.compareTo(entityId) >= 0) {
                    throw new IOException("invalid or duplicate graph entity");
                }
                validateStringCount(input, aliasCount);
                previousEntityId = entityId;
                List<String> aliases = readStrings(input, aliasCount);
                int sourceCount = input.readInt();
                if (sourceCount < 0) {
                    throw new IOException("invalid graph entity provenance count");
                }
                validateStringCount(input, sourceCount);
                List<String> sources = readStrings(input, sourceCount);
                entities.add(new EntityRef(entityId, canonical, aliases, type, sources));
            }
            edges = new ArrayList<>(edgeCount);
            Set<String> edgeIds = new HashSet<>();
            String previousEdgeId = null;
            for (int index = 0; index < edgeCount; index++) {
                String edgeId = input.readUTF();
                final String label = input.readUTF();
                String source = input.readUTF();
                String target = input.readUTF();
                int sourceCount = input.readInt();
                if (sourceCount < 0 || !edgeIds.add(edgeId)
                    || !entityIds.contains(source) || !entityIds.contains(target)
                    || previousEdgeId != null && previousEdgeId.compareTo(edgeId) >= 0) {
                    throw new IOException("invalid or duplicate graph edge");
                }
                validateStringCount(input, sourceCount);
                previousEdgeId = edgeId;
                edges.add(new GraphEdgeRef(edgeId, label, source, target, readStrings(input, sourceCount)));
            }
            for (GraphEdgeRef edge : edges) {
                if (!entityIds.contains(edge.getSourceEntityId())
                    || !entityIds.contains(edge.getTargetEntityId())) {
                    throw new IOException("graph edge references unknown entity");
                }
            }
            if (input.read() != -1) {
                throw new IOException("trailing graph artifact data");
            }
            neighbors = new InMemoryGraphNeighborProvider(edges);
        } catch (RetrievalException error) {
            throw error;
        } catch (Exception error) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                "unable to read graph artifact", error);
        }
    }

    private static List<String> readStrings(DataInputStream input, int count) throws IOException {
        validateStringCount(input, count);
        List<String> values = new ArrayList<>(count);
        Set<String> seen = new HashSet<>();
        for (int index = 0; index < count; index++) {
            String value = input.readUTF();
            if (!seen.add(value)) {
                throw new IOException("duplicate graph reference");
            }
            values.add(value);
        }
        return values;
    }

    private static void validateStringCount(DataInputStream input, int count) throws IOException {
        if (count < 0 || (long) count * 2L > input.available()) {
            throw new IOException("graph string count exceeds remaining artifact data");
        }
    }

    private static RetrievalException notReady(String message) {
        return new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
    }

    public ArtifactIdentity getIdentity() {
        return identity;
    }

    public List<EntityRef> getEntities() {
        return java.util.Collections.unmodifiableList(entities);
    }

    public List<GraphEdgeRef> getEdges() {
        return java.util.Collections.unmodifiableList(edges);
    }

    public GraphNeighborProvider getNeighbors() {
        return neighbors;
    }

    /** Validates that all graph provenance references point at the supplied chunks. */
    public void validateReferences(List<TextChunk> chunks) {
        if (chunks == null) {
            throw notReady("graph fixture chunks are missing");
        }
        Set<String> chunkIds = new HashSet<>();
        for (TextChunk chunk : chunks) {
            if (chunk == null || !chunkIds.add(chunk.getChunkId())) {
                throw notReady("invalid or duplicate fixture chunk");
            }
        }
        for (EntityRef entity : entities) {
            validateChunkReferences(entity.getSourceChunkIds(), chunkIds, "entity " + entity.getEntityId());
        }
        for (GraphEdgeRef edge : edges) {
            validateChunkReferences(edge.getSourceChunkIds(), chunkIds, "edge " + edge.getEdgeId());
        }
    }

    private static void validateChunkReferences(List<String> references, Set<String> chunkIds, String owner) {
        for (String chunkId : references) {
            if (!chunkIds.contains(chunkId)) {
                throw notReady(owner + " references unknown chunk " + chunkId);
            }
        }
    }

    @Override
    public void close() {
        // The artifact is loaded into immutable memory; no resources remain open.
    }
}
