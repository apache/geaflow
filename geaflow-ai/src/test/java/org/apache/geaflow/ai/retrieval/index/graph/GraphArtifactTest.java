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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
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
            assertEquals("e1", reader.getVertices().get(0).getEntityId());
            assertEquals(edges, reader.getNeighbors().neighbors("e1"));
            assertEquals(new GraphVersion(GRAPH, VERSION), reader.getNeighbors().getGraphVersion());
            assertEquals(org.apache.geaflow.ai.retrieval.channel.graph.VersionedGraphView
                .from(reader.toSnapshot()).getFingerprint(), reader.getNeighbors().getFingerprint());
            assertEquals("", reader.getNeighbors().getDeltaDigest());
            assertTrue(reader.getNeighbors().getStatisticsVersion().startsWith("stats-"));
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
    void artifactNeighborIndexPrioritizesWithinTheRequestedScanBudget() throws Exception {
        List<TextChunk> chunks = chunks();
        List<EntityRef> entities = Arrays.asList(new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Weak", "topic"), new EntityRef("e3", "Strong", "topic"));
        List<GraphEdgeRef> edges = Arrays.asList(
            new GraphEdgeRef("a-weak", "weak", "e1", "e2", Collections.singletonList("c1")),
            new GraphEdgeRef("z-strong", "strong", "e1", "e3", Collections.singletonList("c2")));
        Path artifact;
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities, edges)) {
            artifact = tempDir.resolve("graph-v1.bin");
        }

        try (GraphArtifactReader reader = new GraphArtifactReader(artifact, VERSION, VERSION)) {
            GraphNeighborProvider.NeighborBatch batch = reader.getNeighbors().prioritizedNeighbors("e1",
                java.util.Comparator.comparingDouble((GraphEdgeRef edge) ->
                    "strong".equals(edge.getLabel()) ? 0.0 : 1.0)
                    .thenComparing(GraphEdgeRef::getEdgeId), 1, 2);

            assertEquals(2, batch.getExamined());
            assertEquals("z-strong", batch.getEdges().get(0).getEdgeId());
            assertFalse(batch.hasMore());
            assertEquals(edges, reader.getEdges());
            assertEquals(2, reader.getNeighbors().getStatistics().getDegree("e1"));
            assertEquals(Collections.singletonMap("weak", 1),
                reader.getNeighbors().getStatistics().getRelationCounts("e2"));
        }
    }

    @Test
    void persistsGraphProvenanceAndReadsLegacyGraphLayout() throws Exception {
        List<TextChunk> chunks = scopedChunks();
        List<EntityRef> entities = Collections.singletonList(new EntityRef("e1", "Confucius",
            Collections.singletonList("kongzi"), "person", Collections.singletonList("c1"),
            "dataset", "revision", "dev", "sha256:doc-1"));
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef("edge-1", "studies",
            "e1", "e1", Collections.singletonList("c2"), "dataset", "revision", "dev",
            "sha256:doc-2"));
        Path artifact;
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities, edges)) {
            artifact = tempDir.resolve("graph-v1.bin");
        }
        try (GraphArtifactReader reader = new GraphArtifactReader(artifact, VERSION, VERSION)) {
            assertEquals("dataset", reader.getEntities().get(0).getDataset());
            assertEquals("sha256:doc-1", reader.getEntities().get(0).getSourceHash());
            assertEquals("revision", reader.getEdges().get(0).getDatasetVersion());
            assertEquals("sha256:doc-2", reader.getEdges().get(0).getSourceHash());
            reader.validateReferences(chunks);
        }

        Path legacy = tempDir.resolve("legacy-graph.bin");
        writeLegacyArtifact(legacy, chunks, entities, edges);
        try (GraphArtifactReader reader = new GraphArtifactReader(legacy, VERSION, VERSION)) {
            assertEquals(Collections.singletonList("c1"), reader.getEntities().get(0).getSourceChunkIds());
            assertEquals("e1", reader.getEdges().get(0).getSourceEntityId());
        }
    }

    @Test
    void rejectsGraphProvenanceThatDoesNotMatchReferencedChunk() {
        List<TextChunk> chunks = scopedChunks();
        List<EntityRef> entities = Collections.singletonList(new EntityRef("e1", "Confucius",
            Collections.emptyList(), "person", Collections.singletonList("c1"),
            "dataset", "revision", "dev", "sha256:wrong"));
        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(tempDir)
            .build(context(), chunks, entities, Collections.emptyList()));
    }

    @Test
    void persistsDistinctVertexIdsAndMapsThemBackToEntities() throws Exception {
        List<TextChunk> chunks = chunks();
        List<EntityRef> entities = Arrays.asList(new EntityRef("e1", "Confucius", "person"),
            new EntityRef("e2", "Astronomy", "topic"));
        List<GraphVertexRef> vertices = Arrays.asList(new GraphVertexRef("PERSON", "vertex-1", "e1"),
            new GraphVertexRef("TOPIC", "vertex-2", "e2"));
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef("edge-1", "studies",
            "e1", "e2", Collections.singletonList("c2")));
        Path artifact;
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities, vertices, edges)) {
            artifact = tempDir.resolve("graph-v1.bin");
        }
        try (GraphArtifactReader reader = new GraphArtifactReader(artifact, VERSION, VERSION)) {
            assertEquals("vertex-1", reader.getNeighbors().vertex(entities.get(0)).getVertexId());
            assertEquals("vertex-2", reader.getNeighbors().vertex(entities.get(1)).getVertexId());
            assertEquals("e1", reader.getVertices().get(0).getEntityId());
        }
        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(tempDir.resolve("missing"))
            .build(context(), chunks, entities, Collections.singletonList(vertices.get(0)), edges));
        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(tempDir.resolve("duplicate"))
            .build(context(), chunks, entities, Arrays.asList(vertices.get(0),
                new GraphVertexRef("OTHER", "vertex-3", "e1")), edges));
    }

    @Test
    void graphEvidenceKeepsSupportingChunksValidForMultiSourceEdges() throws Exception {
        List<TextChunk> chunks = scopedChunks();
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", Collections.emptyList(), "person",
                Collections.singletonList("c1"), "dataset", "revision", "dev", "sha256:doc-1"),
            new EntityRef("e2", "philosophy", Collections.emptyList(), "topic",
                Collections.singletonList("c2"), "dataset", "revision", "dev", "sha256:doc-2"));
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef("edge-1", "studies",
            "e1", "e2", Arrays.asList("c1", "c2"), "dataset", "revision", "dev",
            org.apache.geaflow.ai.retrieval.model.document.RetrievalIds.aggregateSourceHash(
                Arrays.asList("sha256:doc-1", "sha256:doc-2"))));
        Path artifact;
        try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                 new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities, edges)) {
            artifact = tempDir.resolve("graph-v1.bin");
        }
        RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry();
        try {
            registry.loadGraph(GRAPH, VERSION, VERSION, artifact, chunks);
            org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest request =
                new org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest();
            request.setGraphName(GRAPH);
            request.setGraphVersion(VERSION);
            request.setIndexVersion(VERSION);
            request.setQuery("Confucius");
            request.setMode("GRAPH_ONLY");
            request.setBudget(new org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget(5,
                1000, 5, 100));
            org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse response =
                new RecallService(registry).retrieve(request);

            assertEquals(2, response.getEvidence().size());
            for (org.apache.geaflow.ai.retrieval.model.evidence.Evidence evidence
                : response.getEvidence()) {
                assertEquals(1, evidence.getChunks().size());
                assertEquals(Collections.singletonList(evidence.getChunks().get(0).getChunkId()),
                    evidence.getGraphTraces().get(0).getSupportingChunkIds());
            }
        } finally {
            registry.close();
        }
    }

    @Test
    void builderRejectsUnprovenancedReferencesToScopedChunks() {
        List<TextChunk> chunks = scopedChunks();
        List<EntityRef> entities = Arrays.asList(
            new EntityRef("e1", "Confucius", Collections.emptyList(), "person",
                Collections.singletonList("c1"), "dataset", "revision", "dev", "sha256:doc-1"),
            new EntityRef("e2", "philosophy", Collections.emptyList(), "topic",
                Collections.singletonList("c2"), "dataset", "revision", "dev", "sha256:doc-2"));
        List<GraphEdgeRef> edges = Collections.singletonList(new GraphEdgeRef("edge-1", "studies",
            "e1", "e2", Arrays.asList("c1", "c2")));
        List<GraphEdgeRef> incorrectHashEdges = Collections.singletonList(new GraphEdgeRef("edge-1",
            "studies", "e1", "e2", Arrays.asList("c1", "c2"), "dataset", "revision", "dev",
            "sha256:wrong"));

        assertThrows(IOException.class,
            () -> new OfflineGraphArtifactBuilder(tempDir).build(context(), chunks, entities, edges));
        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(tempDir)
            .build(context(), chunks, entities, incorrectHashEdges));
    }

    @Test
    void buildersPublishIndependentIndexVersionsForTheSameGraph() throws Exception {
        List<TextChunk> chunks = chunks();
        for (String indexVersion : Arrays.asList("index-19", "index-20")) {
            IngestionContext context = new IngestionContext(context().getManifest(),
                new GraphVersion(GRAPH, VERSION), "test", indexVersion);
            Path graphPath;
            try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact graph =
                     new OfflineGraphArtifactBuilder(tempDir).build(context, chunks, entities(),
                         Arrays.asList(new GraphVertexRef("PERSON", "actual-v1", "e1"),
                             new GraphVertexRef("TOPIC", "actual-v2", "e2")), edges())) {
                graphPath = java.nio.file.Paths.get(graph.getMetadata().getArtifactUri());
            }
            Path bm25Path;
            try (org.apache.geaflow.ai.retrieval.index.IndexArtifact index =
                     new org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder(tempDir)
                         .build(context, chunks)) {
                assertEquals(indexVersion, index.getMetadata().getIndexVersion().getVersion());
                assertEquals(VERSION, index.getMetadata().getIndexVersion().getGraphVersion());
                bm25Path = java.nio.file.Paths.get(index.getMetadata().getArtifactUri());
            }
            Path vectorPath;
            java.util.Map<String, float[]> vectors = new java.util.HashMap<>();
            vectors.put("c1", new float[] {1.0F, 0.0F});
            vectors.put("c2", new float[] {0.0F, 1.0F});
            try (org.apache.geaflow.ai.retrieval.index.IndexArtifact index =
                     new org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder(tempDir,
                         vectors, "fixture", VERSION).build(context, chunks)) {
                assertEquals(indexVersion, index.getMetadata().getIndexVersion().getVersion());
                vectorPath = java.nio.file.Paths.get(index.getMetadata().getArtifactUri());
            }
            try (RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry()) {
                registry.loadGraph(GRAPH, VERSION, indexVersion, graphPath, chunks);
                assertTrue(registry.get(GRAPH, VERSION, indexVersion).isReady());
                assertEquals(Arrays.asList("actual-v1", "actual-v2"),
                    retrieveGraphVertexIds(registry, indexVersion));
                registry.load(GRAPH, VERSION, indexVersion, bm25Path, vectorPath, chunks,
                    entities(), edges(), "fixture", VERSION);
                assertEquals(indexVersion, registry.get(GRAPH, VERSION, indexVersion).getIndexVersion());
                assertEquals(Arrays.asList("actual-v1", "actual-v2"),
                    retrieveGraphVertexIds(registry, indexVersion));
            }
        }
    }

    private static List<String> retrieveGraphVertexIds(RetrievalFixtureRegistry registry,
                                                         String indexVersion) {
        org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest request =
            new org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest();
        request.setGraphName(GRAPH);
        request.setGraphVersion(VERSION);
        request.setIndexVersion(indexVersion);
        request.setQuery("Confucius");
        request.setMode("GRAPH_ONLY");
        request.setBudget(new org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget(5,
            1000, 5, 100));
        org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse response =
            new RecallService(registry).retrieve(request);
        return response.getEvidence().get(0).getGraphTraces().get(0).getPath().getVertexIds();
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
    void builderRejectsScopedEntitiesWithoutSourceChunks() {
        EntityRef entity = new EntityRef("e1", "Confucius", Collections.emptyList(), "person",
            Collections.emptyList(), "dataset", "revision", "dev", "sha256:doc-1");

        assertThrows(IOException.class, () -> new OfflineGraphArtifactBuilder(tempDir)
            .build(context(), scopedChunks(), Collections.singletonList(entity),
                Collections.emptyList()));
    }

    @Test
    void registryRejectsGraphHashThatConflictsWithIndexedChunks() throws Exception {
        List<TextChunk> chunks = scopedChunks();
        Path bm25Path;
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact index =
                 new org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder(tempDir)
                     .build(context(), chunks)) {
            bm25Path = java.nio.file.Paths.get(index.getMetadata().getArtifactUri());
        }
        EntityRef incorrectHash = new EntityRef("e1", "Confucius", Collections.emptyList(),
            "person", Collections.singletonList("c1"), "dataset", "revision", "dev",
            "sha256:wrong");
        try (RetrievalFixtureRegistry registry = new RetrievalFixtureRegistry()) {
            assertThrows(org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
                () -> registry.load(GRAPH, VERSION, VERSION, bm25Path,
                    tempDir.resolve("missing-vector"), chunks, Collections.singletonList(incorrectHash),
                    Collections.emptyList(), "fixture", VERSION));
        }
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
            new TextChunk("c1", "doc-1", 0, 0, 10, 2, "Confucius").withSourceUri("fixture://doc-1"),
            new TextChunk("c2", "doc-2", 0, 0, 9, 2, "philosophy").withSourceUri("fixture://doc-2"));
    }

    private static List<TextChunk> scopedChunks() {
        return Arrays.asList(
            new TextChunk("c1", "doc-1", 0, 0, 10, 2, "Confucius", "v1", "sha256:c1",
                "https://example.test/doc-1", "dataset", "revision", "dev", "sha256:doc-1"),
            new TextChunk("c2", "doc-2", 0, 0, 9, 2, "philosophy", "v1", "sha256:c2",
                "https://example.test/doc-2", "dataset", "revision", "dev", "sha256:doc-2"));
    }

    private static void writeLegacyArtifact(Path artifact, List<TextChunk> chunks,
                                            List<EntityRef> entities, List<GraphEdgeRef> edges)
        throws IOException {
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(artifact))) {
            output.writeUTF("GEAFLOW-GRAPH-1");
            output.writeUTF(GRAPH);
            output.writeUTF(VERSION);
            output.writeUTF(VERSION);
            output.writeUTF(org.apache.geaflow.ai.retrieval.index.ArtifactIdentity.fingerprint(chunks));
            output.writeInt(entities.size());
            output.writeInt(edges.size());
            for (EntityRef entity : entities) {
                output.writeUTF(entity.getEntityId());
                output.writeUTF(entity.getCanonicalName());
                output.writeUTF(entity.getType());
                output.writeInt(entity.getAliases().size());
                for (String alias : entity.getAliases()) {
                    output.writeUTF(alias);
                }
                output.writeInt(entity.getSourceChunkIds().size());
                for (String chunkId : entity.getSourceChunkIds()) {
                    output.writeUTF(chunkId);
                }
            }
            for (GraphEdgeRef edge : edges) {
                output.writeUTF(edge.getEdgeId());
                output.writeUTF(edge.getLabel());
                output.writeUTF(edge.getSourceEntityId());
                output.writeUTF(edge.getTargetEntityId());
                output.writeInt(edge.getSourceChunkIds().size());
                for (String chunkId : edge.getSourceChunkIds()) {
                    output.writeUTF(chunkId);
                }
            }
        }
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
