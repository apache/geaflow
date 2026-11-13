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

package org.apache.geaflow.ai.retrieval.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.SourceDocument;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;
import org.junit.jupiter.api.Test;

class RetrievalProvenanceContractTest {

    @Test
    void ingestionCarriesDocumentProvenanceIntoChunkAndCitation() {
        SourceDocument document = new SourceDocument("doc-1", "hotpotqa", "release-3", "dev",
            "Title", "https://example.test/doc-1", "sha256:document-1");
        TextChunk chunk = new TextChunk("chunk-1", "doc-1", 0, 3, 8, 2, "Alice", "v1",
            "sha256:chunk-1").withSourceDocument(document);
        SourceRef source = SourceRef.fromDocument(document, 3, 8);

        assertEquals("hotpotqa", chunk.getDataset());
        assertEquals("release-3", chunk.getDatasetVersion());
        assertEquals("dev", chunk.getSplit());
        assertEquals("sha256:document-1", chunk.getSourceHash());
        assertEquals(source.getSourceHash(), chunk.getSourceHash());
        assertEquals("https://example.test/doc-1", chunk.getSourceUri());
    }

    @Test
    void evidenceSchemaAndVersionsRoundTripAndRequireExplicitSchema() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef source = scopedSource("doc-1", "release-3");
        Evidence evidence = new Evidence("e-1", EvidenceKind.CHUNK, "Alice",
            Collections.singletonList(chunk), Collections.emptyList(),
            Collections.singletonList(source), Collections.emptyMap(), 0.5, null, 1,
            Collections.emptyList(), Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa", "release-3",
            "dev", "graph-7", "index-19");

        Evidence restored = RetrievalModelJson.fromJson(RetrievalModelJson.toJson(evidence),
            Evidence.class);
        assertEquals(evidence, restored);
        assertEquals(Evidence.CURRENT_SCHEMA_VERSION, restored.getSchemaVersion());
        assertEquals("release-3", restored.getDatasetVersion());
        assertEquals("graph-7", restored.getGraphVersion());
        assertEquals("index-19", restored.getIndexVersion());
        assertThrows(com.google.gson.JsonParseException.class,
            () -> RetrievalModelJson.fromJson("{\"kind\":\"CHUNK\"}", Evidence.class));
    }

    @Test
    void evidenceRejectsConflictingDatasetGraphAndIndexVersions() {
        TextChunk first = scopedChunk("chunk-1", "doc-1", "release-3");
        TextChunk second = scopedChunk("chunk-2", "doc-2", "release-4");
        SourceRef firstSource = scopedSource("doc-1", "release-3");
        SourceRef secondSource = scopedSource("doc-2", "release-4");

        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{first, second}, new SourceRef[]{firstSource, secondSource},
            "v7", "v7"));
        assertEquals("graph-v7", evidence(new TextChunk[]{first}, new SourceRef[]{firstSource},
            "graph-v7", null).getGraphVersion());
        assertEquals("index-v19", evidence(new TextChunk[]{first}, new SourceRef[]{firstSource},
            null, "index-v19").getIndexVersion());
        assertThrows(RetrievalModelValidationException.class,
            () -> new Evidence("e-1", EvidenceKind.CHUNK, "Alice", Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), 0.5, null, 1, Collections.emptyList(), 99,
                null, null, null, null, null));
    }

    @Test
    void evidenceRejectsSourceChunkDocumentUriOffsetAndHashConflicts() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");

        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{chunk}, new SourceRef[]{new SourceRef("doc-2",
                "https://example.test/doc-1", 3, 8, "hotpotqa", "release-3", "dev",
                "sha256:document-1")}, "v7", "v7"));
        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{chunk}, new SourceRef[]{new SourceRef("doc-1",
                "https://example.test/other", 3, 8, "hotpotqa", "release-3", "dev",
                "sha256:document-1")}, "v7", "v7"));
        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{chunk}, new SourceRef[]{new SourceRef("doc-1",
                "https://example.test/doc-1", 2, 8, "hotpotqa", "release-3", "dev",
                "sha256:document-1")}, "v7", "v7"));
        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{chunk}, new SourceRef[]{new SourceRef("doc-1",
                "https://example.test/doc-1", 3, 8, "hotpotqa", "release-3", "dev",
                "sha256:wrong")}, "v7", "v7"));
    }

    @Test
    void evidenceRejectsConflictingCitationsForTheSameDocumentSpan() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef matching = scopedSource("doc-1", "release-3");
        SourceRef conflicting = new SourceRef("doc-1", "https://example.test/other", 3, 8,
            "hotpotqa", "release-3", "dev", "sha256:document-1");

        assertThrows(RetrievalModelValidationException.class, () -> evidence(
            new TextChunk[]{chunk}, new SourceRef[]{matching, conflicting}, "v7", "v7"));
    }

    @Test
    void currentSchemaRequiresProvenanceForEntitiesAndVerifiesCompleteSourceHashes() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef source = scopedSource("doc-1", "release-3");
        EntityRef missingScope = new EntityRef("entity-1", "Alice", "PERSON");
        EntityRef missingSources = new EntityRef("entity-1", "Alice", Collections.emptyList(),
            "PERSON", Collections.emptyList(), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        EntityRef wrongHash = new EntityRef("entity-1", "Alice", Collections.emptyList(),
            "PERSON", Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:wrong");
        EntityRef validEntity = new EntityRef("entity-1", "Alice", Collections.emptyList(),
            "PERSON", Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");

        assertThrows(RetrievalModelValidationException.class, () -> currentEvidence(
            EvidenceKind.ENTITY, Collections.singletonList(chunk),
            Collections.singletonList(missingScope), Collections.singletonList(source),
            Collections.emptyList()));
        assertThrows(RetrievalModelValidationException.class, () -> currentEvidence(
            EvidenceKind.ENTITY, Collections.singletonList(chunk),
            Collections.singletonList(missingSources), Collections.singletonList(source),
            Collections.emptyList()));
        assertThrows(RetrievalModelValidationException.class, () -> currentEvidence(
            EvidenceKind.CHUNK, Collections.singletonList(chunk),
            Collections.singletonList(wrongHash), Collections.singletonList(source),
            Collections.emptyList()));

        assertEquals(Collections.singletonList(validEntity), currentEvidence(EvidenceKind.ENTITY,
            Collections.singletonList(chunk), Collections.singletonList(validEntity),
            Collections.singletonList(source), Collections.emptyList())
            .getEntities());
    }

    @Test
    void graphEdgeHashIsCheckedWhenAllSourcesArePresentAndDeferredForPartialEvidence() {
        TextChunk first = scopedChunk("chunk-1", "doc-1", "release-3");
        TextChunk second = new TextChunk("chunk-2", "doc-2", 0, 3, 8, 2, "Alice", "v1",
            "sha256:chunk-2", "https://example.test/doc-2", "hotpotqa", "release-3", "dev",
            "sha256:document-2");
        SourceRef firstSource = scopedSource("doc-1", "release-3");
        SourceRef secondSource = new SourceRef("doc-2", "https://example.test/doc-2", 3, 8,
            "hotpotqa", "release-3", "dev", "sha256:document-2");
        EntityRef anchor = new EntityRef("entity-1", "Alice", Collections.emptyList(), "PERSON",
            Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        GraphVertexRef seed = new GraphVertexRef("PERSON", "vertex-1", "entity-1", "hotpotqa",
            "release-3", "dev", "sha256:document-1");
        GraphVertexRef endpoint = new GraphVertexRef("PERSON", "vertex-2", "entity-2", "hotpotqa",
            "release-3", "dev", "sha256:document-2");
        String aggregateHash = RetrievalIds.aggregateSourceHash(
            Arrays.asList("sha256:document-1", "sha256:document-2"));
        GraphEdgeRef validEdge = new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-2",
            Arrays.asList("chunk-1", "chunk-2"), "hotpotqa", "release-3", "dev", aggregateHash);
        GraphPathRef path = new GraphPathRef(Arrays.asList(seed, endpoint),
            Collections.singletonList(validEdge), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", path, Collections.singletonList("chunk-1"));
        GraphEdgeRef wrongHashEdge = new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-2",
            Arrays.asList("chunk-1", "chunk-2"), "hotpotqa", "release-3", "dev", "sha256:wrong");
        GraphPathRef wrongHashPath = new GraphPathRef(Arrays.asList(seed, endpoint),
            Collections.singletonList(wrongHashEdge), false);
        GraphEvidenceTrace wrongHashTrace = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", wrongHashPath, Collections.singletonList("chunk-1"));

        assertThrows(RetrievalModelValidationException.class, () -> currentEvidence(
            EvidenceKind.GRAPH_PATH, Arrays.asList(first, second), Collections.singletonList(anchor),
            Arrays.asList(firstSource, secondSource),
            Collections.singletonList(wrongHashTrace)));
        Evidence partial = currentEvidence(EvidenceKind.GRAPH_PATH, Collections.singletonList(first),
            Collections.singletonList(anchor), Collections.singletonList(firstSource),
            Collections.singletonList(trace));
        assertEquals("v7", partial.getGraphVersion());
    }

    @Test
    void evidenceRejectsGraphTraceVersionAndSupportingChunkConflicts() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef source = scopedSource("doc-1", "release-3");
        EntityRef anchor = new EntityRef("entity-1", "Alice", Collections.emptyList(), "PERSON",
            Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        GraphVertexRef seed = new GraphVertexRef("PERSON", "vertex-1", "entity-1", "hotpotqa",
            "release-3", "dev", "sha256:document-1");
        GraphPathRef path = new GraphPathRef(Collections.singletonList(seed),
            Collections.emptyList(), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", path, Collections.singletonList("chunk-2"));

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.CHUNK, "Alice", Collections.singletonList(chunk),
            Collections.singletonList(anchor), Collections.singletonList(source),
            Collections.emptyMap(), 0.5, null, 1,
            Collections.singletonList(trace), Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa", "release-3",
            "dev", "v8", "v8"));

        GraphEvidenceTrace validVersionWrongChunk = new GraphEvidenceTrace("q-1",
            RetrievalMode.GRAPH_ONLY, "CANONICAL", 1.0, "v7", path,
            Collections.singletonList("chunk-2"));
        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.CHUNK, "Alice", Collections.singletonList(chunk),
            Collections.singletonList(anchor), Collections.singletonList(source),
            Collections.emptyMap(), 0.5, null, 1,
            Collections.singletonList(validVersionWrongChunk), Evidence.CURRENT_SCHEMA_VERSION,
            "hotpotqa", "release-3", "dev", "v7", "index-19"));
    }

    @Test
    void currentSchemaRequiresSourceProvenanceWhileLegacySchemaRemainsReadable() {
        TextChunk legacyChunk = new TextChunk("chunk-1", "doc-1", 0, 3, 8, 2, "Alice");

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.CHUNK, "Alice", Collections.singletonList(legacyChunk),
            Collections.emptyList(), Collections.emptyList(),
            Collections.emptyMap(), 0.5, null, 1, Collections.emptyList(),
            Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa", "release-3", "dev", null, null));

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.CHUNK, "Alice", Collections.singletonList(legacyChunk),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), 0.5, null, 1,
            Collections.emptyList(), 1, null, null, null, null, null));
    }

    @Test
    void currentSchemaRejectsGraphSeedFromAnotherDatasetScope() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef source = scopedSource("doc-1", "release-3");
        GraphVertexRef foreignSeed = new GraphVertexRef("PERSON", "vertex-1", "entity-1",
            "other", "release-3", "dev", "sha256:other");
        GraphEvidenceTrace trace = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", new GraphPathRef(
                Collections.singletonList(foreignSeed), Collections.emptyList(), false),
            Collections.singletonList("chunk-1"));
        EntityRef anchor = new EntityRef("entity-1", "Alice", Collections.emptyList(), "PERSON",
            Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.CHUNK, "Alice", Collections.singletonList(chunk),
            Collections.singletonList(anchor),
            Collections.singletonList(source), Collections.emptyMap(), 0.5, null,
            1, Collections.singletonList(trace), Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa",
            "release-3", "dev", "v7", "index-19"));
    }

    @Test
    void currentTraceSupportingChunksMustBackTheGraphPath() {
        TextChunk firstChunk = new TextChunk("chunk-1", "doc-1", 0, 0, 5, 1, "Alice", "v1",
            "sha256:chunk-1", "https://example.test/doc-1", "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        TextChunk secondChunk = new TextChunk("chunk-2", "doc-2", 0, 0, 3, 1, "Bob", "v1",
            "sha256:chunk-2", "https://example.test/doc-2", "hotpotqa", "release-3", "dev",
            "sha256:document-2");
        SourceRef firstSource = new SourceRef("doc-1", "https://example.test/doc-1", 0, 5,
            "hotpotqa", "release-3", "dev", "sha256:document-1");
        SourceRef secondSource = new SourceRef("doc-2", "https://example.test/doc-2", 0, 3,
            "hotpotqa", "release-3", "dev", "sha256:document-2");
        EntityRef seedEntity = new EntityRef("entity-1", "Alice", Collections.emptyList(),
            "PERSON", Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        GraphVertexRef seed = new GraphVertexRef("PERSON", "vertex-1", "entity-1", "hotpotqa",
            "release-3", "dev", "sha256:document-1");
        GraphVertexRef endpoint = new GraphVertexRef("PERSON", "vertex-2", "entity-2", "hotpotqa",
            "release-3", "dev", "sha256:document-2");
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-2",
            Collections.singletonList("chunk-1"), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        GraphPathRef path = new GraphPathRef(Arrays.asList(seed, endpoint),
            Collections.singletonList(edge), false);
        GraphEvidenceTrace unsupported = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", path, Collections.singletonList("chunk-2"));

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.GRAPH_PATH, "Alice and Bob", Arrays.asList(firstChunk, secondChunk),
            Collections.singletonList(seedEntity),
            Arrays.asList(firstSource, secondSource), Collections.emptyMap(), 0.5, null, 1,
            Collections.singletonList(unsupported), Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa",
            "release-3", "dev", "v7", "index-19"));

        GraphEdgeRef edgeWithoutSources = new GraphEdgeRef("edge-1", "knows", "entity-1",
            "entity-2", Collections.emptyList(), "hotpotqa", "release-3", "dev",
            "sha256:document-1");
        GraphPathRef pathWithoutEdgeSources = new GraphPathRef(Arrays.asList(seed, endpoint),
            Collections.singletonList(edgeWithoutSources), false);
        GraphEvidenceTrace entityFallback = new GraphEvidenceTrace("q-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "v7", pathWithoutEdgeSources,
            Collections.singletonList("chunk-1"));
        Evidence valid = new Evidence("e-1", EvidenceKind.GRAPH_PATH, "Alice and Bob",
            Arrays.asList(firstChunk, secondChunk), Collections.singletonList(seedEntity),
            Arrays.asList(firstSource, secondSource),
            Collections.emptyMap(), 0.5, null, 1, Collections.singletonList(entityFallback),
            Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa", "release-3", "dev", "v7", "index-19");

        assertEquals(Collections.singletonList("chunk-1"),
            valid.getGraphTraces().get(0).getSupportingChunkIds());
    }

    @Test
    void stableIdsUseNormalizedSourceIdentityAndChunkContent() {
        String first = RetrievalIds.sourceDocumentId("hotpotqa", "release-3", "dev",
            "HTTPS://EXAMPLE.TEST:443/a/../doc-1", "sha256:document-1");
        String equivalent = RetrievalIds.sourceDocumentId("hotpotqa", "release-3", "dev",
            "https://example.test/doc-1", "sha256:document-1");
        String encodedSlash = RetrievalIds.normalizeSourceUri("https://example.test/a%2Fb");
        String pathSeparator = RetrievalIds.normalizeSourceUri("https://example.test/a/b");
        String relativeLowerEscape = RetrievalIds.normalizeSourceUri("/tmp/a%2fb");
        String relativeUpperEscape = RetrievalIds.normalizeSourceUri("/tmp/a%2Fb");
        String encodedSlashId = RetrievalIds.sourceDocumentId("hotpotqa", "release-3", "dev",
            "https://example.test/a%2Fb", "sha256:document-1");
        String pathSeparatorId = RetrievalIds.sourceDocumentId("hotpotqa", "release-3", "dev",
            "https://example.test/a/b", "sha256:document-1");
        SourceDocument equivalentDocument = new SourceDocument("doc-1", "hotpotqa", "release-3",
            "dev", "https://EXAMPLE.TEST:443/a/../doc-1", "sha256:document-1");
        SourceDocument normalizedDocument = new SourceDocument("doc-1", "hotpotqa",
            "release-3", "dev", "https://example.test/doc-1", "sha256:document-1");
        TextChunk equivalentUriChunk = new TextChunk("chunk-1", "doc-1", 0, 3, 8, 2, "Alice",
            "v1", "sha256:chunk-1", "HTTPS://EXAMPLE.TEST:443/a/../doc-1", "hotpotqa",
            "release-3", "dev", "sha256:document-1");

        assertEquals(first, equivalent);
        assertNotEquals(encodedSlash, pathSeparator);
        assertNotEquals(encodedSlashId, pathSeparatorId);
        assertEquals(relativeLowerEscape, relativeUpperEscape);
        assertTrue(equivalentDocument.sameIdentityAs(normalizedDocument));
        assertTrue(scopedChunk("chunk-1", "doc-1", "release-3")
            .sameIdentityAs(equivalentUriChunk));
        assertEquals(ArtifactIdentity.fingerprint(Collections.singletonList(
                scopedChunk("chunk-1", "doc-1", "release-3"))),
            ArtifactIdentity.fingerprint(Collections.singletonList(equivalentUriChunk)));
        assertEquals(RetrievalIds.normalizeSourceUri("FILE:///tmp/a/../source"),
            RetrievalIds.normalizeSourceUri("file:///tmp/source"));
        assertTrue(first.startsWith("doc-"));
        assertEquals(RetrievalIds.chunkId("doc-1", "v1", 0, 0, 5, "Alice"),
            RetrievalIds.chunkId("doc-1", "v1", 0, 0, 5, "Alice"));
        assertTrue(!RetrievalIds.chunkId("doc-1", "v1", 0, 0, 5, "Alice")
            .equals(RetrievalIds.chunkId("doc-1", "v1", 0, 0, 5, "alice")));
        assertEquals("5:doc-1chunk-1", RetrievalIds.chunkEvidenceId("doc-1", "chunk-1"));
    }

    @Test
    void chunkArtifactIdentityIncludesDatasetAndSourceProvenance() {
        TextChunk original = scopedChunk("chunk-1", "doc-1", "release-3");
        TextChunk changed = new TextChunk("chunk-1", "doc-1", 0, 3, 8, 2, "Alice", "v1",
            "sha256:chunk-1", "https://example.test/doc-1", "hotpotqa", "release-4", "dev",
            "sha256:document-1");
        ArtifactIdentity identity = new ArtifactIdentity("graph", "v7", "v7",
            ArtifactIdentity.fingerprint(Collections.singletonList(original)));

        assertNotEquals(ArtifactIdentity.fingerprint(Collections.singletonList(original)),
            ArtifactIdentity.fingerprint(Collections.singletonList(changed)));
        assertThrows(org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            () -> identity.validate("graph", "v7", "v7", Collections.singletonList(changed)));
    }

    @Test
    void artifactIdentityKeepsGraphAndIndexVersionsIndependent() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        ArtifactIdentity identity = new ArtifactIdentity("graph", "graph-v7", "index-v19",
            ArtifactIdentity.fingerprint(Collections.singletonList(chunk)));

        identity.validate("graph", "graph-v7", "index-v19", Collections.singletonList(chunk));
        assertThrows(org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            () -> identity.validate("graph", "index-v19", "graph-v7",
                Collections.singletonList(chunk)));
    }

    @Test
    void aggregateSourceHashIsStableAcrossSourceOrder() {
        String forward = RetrievalIds.aggregateSourceHash(
            Arrays.asList("sha256:document-1", "sha256:document-2"));
        String reverse = RetrievalIds.aggregateSourceHash(
            Arrays.asList("sha256:document-2", "sha256:document-1"));

        assertEquals(forward, reverse);
        assertNotEquals("sha256:document-1", forward);
    }

    @Test
    void legacyArtifactFingerprintIsAcceptedOnlyWithoutNewProvenance() throws Exception {
        TextChunk legacy = new TextChunk("chunk-1", "doc-1", 0, 3, 8, 2, "Alice", "v1",
            "sha256:chunk-1", "https://example.test/doc-1");
        TextChunk scoped = legacy.withSourceDocument(new SourceDocument("doc-1", "hotpotqa",
            "release-3", "dev", "https://example.test/doc-1", "sha256:document-1"));
        ArtifactIdentity oldArtifact = new ArtifactIdentity("graph", "v7", "v7",
            legacyFingerprint(legacy));

        oldArtifact.validate("graph", "v7", "v7", Collections.singletonList(legacy));
        assertThrows(org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            () -> oldArtifact.validate("graph", "v7", "v7", Collections.singletonList(scoped)));
    }

    @Test
    void currentEvidenceRequiresCompleteTraceAndConnectedPathReferences() {
        TextChunk chunk = scopedChunk("chunk-1", "doc-1", "release-3");
        SourceRef source = scopedSource("doc-1", "release-3");
        GraphPathRef path = new GraphPathRef(Collections.singletonList(
            new GraphVertexRef("PERSON", "vertex-1", "entity-1", "hotpotqa", "release-3",
                "dev", "sha256:document-1")), Collections.emptyList(), false);

        assertThrows(RetrievalModelValidationException.class, () -> new Evidence("e-1",
            EvidenceKind.GRAPH_PATH, "Alice", Collections.singletonList(chunk), Collections.emptyList(),
            Collections.singletonList(source), Collections.emptyMap(),
            0.5, null, 1, Collections.emptyList(), Evidence.CURRENT_SCHEMA_VERSION,
            "hotpotqa", "release-3", "dev", "v7", "index-19"));

        GraphVertexRef seed = new GraphVertexRef("PERSON", "vertex-1", "entity-1", "hotpotqa",
            "release-3", "dev", "sha256:document-1");
        GraphVertexRef endpoint = new GraphVertexRef("PERSON", "unrelated", "entity-2", "hotpotqa",
            "release-3", "dev", "sha256:document-1");
        assertThrows(RetrievalModelValidationException.class, () -> new GraphPathRef(
            Arrays.asList(seed, endpoint), Collections.singletonList(
                new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-3")), false));
    }

    private static Evidence evidence(TextChunk[] chunks, SourceRef[] sources, String graphVersion,
                                     String indexVersion) {
        return new Evidence("e-1", EvidenceKind.CHUNK, "Alice", java.util.Arrays.asList(chunks),
            Collections.emptyList(), java.util.Arrays.asList(sources), Collections.emptyMap(), 0.5, null, 1,
            Collections.emptyList(), Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa", "release-3",
            "dev", graphVersion, indexVersion);
    }

    private static Evidence currentEvidence(EvidenceKind kind, java.util.List<TextChunk> chunks,
                                            java.util.List<EntityRef> entities,
                                            java.util.List<SourceRef> sources,
                                            java.util.List<GraphEvidenceTrace> traces) {
        return new Evidence("e-1", kind, "Alice", chunks, entities, sources,
            Collections.emptyMap(), 0.5, null, 1, traces, Evidence.CURRENT_SCHEMA_VERSION, "hotpotqa",
            "release-3", "dev", "v7", "index-19");
    }

    private static TextChunk scopedChunk(String chunkId, String documentId, String version) {
        return new TextChunk(chunkId, documentId, 0, 3, 8, 2, "Alice", "v1", "sha256:chunk-1",
            "https://example.test/doc-1", "hotpotqa", version, "dev", "sha256:document-1");
    }

    private static SourceRef scopedSource(String documentId, String version) {
        return new SourceRef(documentId, "https://example.test/doc-1", 3, 8, "hotpotqa", version,
            "dev", "sha256:document-1");
    }

    private static String legacyFingerprint(TextChunk chunk) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
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
        StringBuilder result = new StringBuilder();
        for (byte value : digest.digest()) {
            result.append(String.format(java.util.Locale.ROOT, "%02x", value & 255));
        }
        return result.toString();
    }

    private static void append(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(ByteBuffer.allocate(4).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
