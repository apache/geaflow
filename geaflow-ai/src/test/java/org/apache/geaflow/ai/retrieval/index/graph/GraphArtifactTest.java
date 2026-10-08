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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.service.RecallService;
import org.apache.geaflow.ai.retrieval.service.RetrievalFixtureRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GraphArtifactTest {
    private static final String GRAPH = "artifact-graph";
    private static final String VERSION = "v1";

    @TempDir
    Path tempDir;

    @Test
    void builderAndReaderRoundTripDeterministicGraphAndRegistryRetrievesIt() throws Exception {
        List<TextChunk> chunks = chunks();
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e2", "Astronomy", "topic"),
            new EntityRef("e1", "Confucius", Collections.singletonList("kongzi"), "person",
                Collections.singletonList("c1")));
        List<GraphEdgeRef> edges = Collections.singletonList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Collections.singletonList("c2")));
        Path firstDirectory = tempDir.resolve("first");
        Path secondDirectory = tempDir.resolve("second");
        Path firstArtifact;
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(firstDirectory).build(context(), chunks, entities, edges)) {
            firstArtifact = firstDirectory.resolve("graph-v1.bin");
        }
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(secondDirectory).build(context(), chunks,
                     Arrays.asList(entities.get(1), entities.get(0)), edges)) {
            // A different input order must produce the same published bytes.
        }
        assertTrue(Arrays.equals(Files.readAllBytes(firstArtifact),
            Files.readAllBytes(secondDirectory.resolve("graph-v1.bin"))));

        try (GraphArtifactReader reader = new GraphArtifactReader(firstArtifact, VERSION, VERSION)) {
            reader.getIdentity().validate(GRAPH, VERSION, VERSION, chunks);
            assertEquals(Arrays.asList("e1", "e2"), Arrays.asList(
                reader.getEntities().get(0).getEntityId(), reader.getEntities().get(1).getEntityId()));
            assertEquals(Collections.singletonList("kongzi"), reader.getEntities().get(0).getAliases());
            assertEquals(Collections.singletonList("c1"), reader.getEntities().get(0).getSourceChunkIds());
            assertEquals(edges, reader.getNeighbors().neighbors("e1"));
            reader.validateReferences(chunks);
            assertThrows(RetrievalException.class, () -> reader.getIdentity().validate(
                GRAPH, VERSION, VERSION, Collections.singletonList(
                    new TextChunk("c1", "doc-1", 0, 0, 1, 1, "changed"))));
        }

        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        try {
            registry.loadGraph(GRAPH, VERSION, VERSION, firstArtifact, chunks);
            RecallService service = new RecallService(registry);
            org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest request =
                new org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest();
            request.setGraphName(GRAPH);
            request.setGraphVersion(VERSION);
            request.setIndexVersion(VERSION);
            request.setQuery("Confucius");
            request.setMode("GRAPH_ONLY");
            request.setBudget(new org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget(5, 1000, 5, 100));
            org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse response = service.retrieve(request);
            assertEquals(1, response.getEvidence().size());
            assertEquals("c2", response.getEvidence().get(0).getChunks().get(0).getChunkId());
            assertEquals(VERSION, response.getTrace().getGraphVersion());
        } finally {
            registry.close();
        }
    }

    @Test
    void rejectsVersionCorruptionAndInvalidBuildReferences() throws Exception {
        List<TextChunk> chunks = chunks();
        Path artifact = tempDir.resolve("graph-v1.bin");
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities(), edges())) {
            // The graph artifact is published for reader validation below.
        }
        assertArtifactError(RetrievalErrorCode.INDEX_NOT_READY,
            () -> new GraphArtifactReader(artifact, "v2", VERSION));

        Path corrupt = tempDir.resolve("corrupt.bin");
        Files.write(corrupt, new byte[] {1, 2, 3});
        assertArtifactError(RetrievalErrorCode.INDEX_NOT_READY,
            () -> new GraphArtifactReader(corrupt, VERSION, VERSION));

        List<GraphEdgeRef> unknownEndpoint = Collections.singletonList(
            new GraphEdgeRef("bad-edge", "studies", "missing", "e2", Collections.singletonList("c2")));
        assertThrows(IOException.class,
            () -> new OfflineGraphArtifactBuilder(tempDir.resolve("invalid-endpoint"))
                .build(context(), chunks, entities(), unknownEndpoint));
        List<GraphEdgeRef> unknownChunk = Collections.singletonList(
            new GraphEdgeRef("bad-edge", "studies", "e1", "e2", Collections.singletonList("missing")));
        assertThrows(IOException.class,
            () -> new OfflineGraphArtifactBuilder(tempDir.resolve("invalid-chunk"))
                .build(context(), chunks, entities(), unknownChunk));
        List<EntityRef> unknownEntityChunk = Collections.singletonList(new EntityRef(
            "e1", "Confucius", Collections.emptyList(), "person", Collections.singletonList("missing")));
        assertThrows(IOException.class,
            () -> new OfflineGraphArtifactBuilder(tempDir.resolve("invalid-entity-chunk"))
                .build(context(), chunks, unknownEntityChunk, Collections.emptyList()));
    }

    @Test
    void rejectsPathTraversalVersionBeforeCreatingOutputDirectory() {
        Path output = tempDir.resolve("unsafe-output");
        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(output).build(
            context("../outside"), chunks(), entities(), edges()));
        assertTrue(Files.notExists(output));
    }

    @Test
    void rejectsCountsThatCannotFitInArtifactBeforeAllocating() throws Exception {
        List<TextChunk> chunks = chunks();
        Path artifact = tempDir.resolve("graph-v1.bin");
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities(), edges())) {
            // Build a valid artifact to mutate its count fields below.
        }
        byte[] original = Files.readAllBytes(artifact);
        int[] offsets = graphCountOffsets(original);

        byte[] invalidEntityCount = original.clone();
        ByteBuffer.wrap(invalidEntityCount).putInt(offsets[0], Integer.MAX_VALUE);
        Path invalidEntityArtifact = tempDir.resolve("invalid-entity-count.bin");
        Files.write(invalidEntityArtifact, invalidEntityCount);
        assertArtifactError(RetrievalErrorCode.INDEX_NOT_READY,
            () -> new GraphArtifactReader(invalidEntityArtifact, VERSION, VERSION));

        byte[] invalidAliasCount = original.clone();
        ByteBuffer.wrap(invalidAliasCount).putInt(offsets[1], Integer.MAX_VALUE);
        Path invalidAliasArtifact = tempDir.resolve("invalid-alias-count.bin");
        Files.write(invalidAliasArtifact, invalidAliasCount);
        assertArtifactError(RetrievalErrorCode.INDEX_NOT_READY,
            () -> new GraphArtifactReader(invalidAliasArtifact, VERSION, VERSION));
    }

    @Test
    void reusesIdenticalPublishedArtifactAndRejectsDifferentContent() throws Exception {
        List<TextChunk> chunks = chunks();
        OfflineGraphArtifactBuilder builder = new OfflineGraphArtifactBuilder(tempDir);
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 builder.build(context(), chunks, entities(), edges())) {
            // Publish initial content.
        }
        Path published = tempDir.resolve("graph-v1.bin");
        byte[] original = Files.readAllBytes(published);
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 builder.build(context(), chunks, entities(), edges())) {
            // Identical content is accepted as an idempotent rebuild.
        }
        assertTrue(Arrays.equals(original, Files.readAllBytes(published)));

        List<GraphEdgeRef> changed = Collections.singletonList(
            new GraphEdgeRef("edge-2", "teaches", "e1", "e2", Collections.singletonList("c2")));
        assertThrows(IOException.class, () -> builder.build(context(), chunks, entities(), changed));
        assertNotEquals(0, Files.size(published));
    }

    private static void assertArtifactError(RetrievalErrorCode code,
                                            org.junit.jupiter.api.function.Executable operation) {
        RetrievalException error = assertThrows(RetrievalException.class, operation);
        assertEquals(code, error.getCode());
    }

    private static int[] graphCountOffsets(byte[] artifact) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(artifact))) {
            for (int index = 0; index < 5; index++) {
                input.readUTF();
            }
            int entityCountOffset = artifact.length - input.available();
            input.readInt();
            input.readInt();
            input.readUTF();
            input.readUTF();
            input.readUTF();
            int aliasCountOffset = artifact.length - input.available();
            return new int[] {entityCountOffset, aliasCountOffset};
        }
    }

    private static List<TextChunk> chunks() {
        return Arrays.asList(
            new TextChunk("c1", "doc-1", 0, 0, 10, 2, "Confucius"),
            new TextChunk("c2", "doc-2", 0, 0, 9, 2, "philosophy"));
    }

    private static List<EntityRef> entities() {
        return Arrays.asList(
            new EntityRef("e1", "Confucius", Collections.singletonList("kongzi"), "person",
                Collections.singletonList("c1")),
            new EntityRef("e2", "Astronomy", "topic"));
    }

    private static List<GraphEdgeRef> edges() {
        return Collections.singletonList(
            new GraphEdgeRef("edge-1", "studies", "e1", "e2", Collections.singletonList("c2")));
    }

    private IngestionContext context() {
        return context(VERSION);
    }

    private IngestionContext context(String graphVersion) {
        DatasetManifest manifest = new DatasetManifest(VERSION, "dataset", "revision", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 7L);
        return new IngestionContext(manifest, new GraphVersion(GRAPH, graphVersion), "test");
    }
}
