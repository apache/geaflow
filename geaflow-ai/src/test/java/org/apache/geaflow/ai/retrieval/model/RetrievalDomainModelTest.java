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

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.model.document.SourceDocument;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for retrieval domain models, validation rules, and JSON round-tripping.
 */
public class RetrievalDomainModelTest {

    private static final Gson GSON = new Gson();

    @Test
    public void modelsAreValueObjectsAndDefensivelyCopyCollections() {
        java.util.List<String> aliases = Arrays.asList("Kong Fuzi", "Master Kong");
        EntityRef entity = new EntityRef("entity-1", "Confucius", aliases,
            "PERSON", Collections.singletonList("chunk-1"));

        Assertions.assertEquals(Arrays.asList("Kong Fuzi", "Master Kong"), entity.getAliases());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> entity.getAliases().add("孔子"));
        Assertions.assertEquals(entity, new EntityRef("entity-1", "Confucius",
            Arrays.asList("Master Kong", "Kong Fuzi"), "PERSON",
            Collections.singletonList("chunk-1")));
        Assertions.assertTrue(entity.sameIdentityAs(new EntityRef("entity-1", "Confucius",
            Collections.emptyList(), "PERSON", Collections.emptyList())));

        SourceDocument document = new SourceDocument("doc-1", "dataset", "v1", "dev",
            "Title", "file:///doc", "hash");
        Assertions.assertTrue(document.sameIdentityAs(new SourceDocument("doc-1", "dataset",
            "v1", "dev", "Other title", "file:///doc", "hash")));

        TextChunk chunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 1, "text",
            "policy-v1", "hash");
        Assertions.assertTrue(chunk.sameIdentityAs(new TextChunk("chunk-1", "doc-1", 0,
            0, 4, 99, "text", "policy-v1", "hash")));

        GraphVertexRef vertex = new GraphVertexRef("person", "vertex-1", "entity-1");
        Assertions.assertFalse(vertex.sameIdentityAs(new GraphVertexRef("company", "vertex-1",
            "entity-1")));
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-2",
            Collections.singletonList("chunk-1"));
        Assertions.assertTrue(edge.sameIdentityAs(new GraphEdgeRef("edge-1", "knows",
            "entity-1", "entity-2", Collections.singletonList("chunk-2"))));

        Assertions.assertTrue(new GraphVersion("graph", "v1")
            .sameIdentityAs(new GraphVersion("graph", "v1")));
        Assertions.assertFalse(new IndexVersion("bm25", "v1", "graph-v1")
            .sameIdentityAs(new IndexVersion("vector", "v1", "graph-v1")));
    }

    @Test
    public void validatesRequiredFieldsAndRanges() {
        Assertions.assertThrows(NullPointerException.class,
            () -> new GraphVersion(null, "v1"));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new TextChunk("chunk-1", "doc-1", 0, 8, 2, 1, "text", null, null));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new ChannelScore("bm25", 1.0, 0.5, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new SourceRef("doc-1", "file:///doc", 4, 2));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new Evidence(" ", EvidenceKind.CHUNK, null, null, null, null, null, null,
                null, null));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new TextChunk("chunk-1", "doc-1", 0, 0, 1, 1, "text", " ", null));
    }

    @Test
    public void evidenceMergesChannelsByFieldsNotScores() {
        TextChunk chunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 1,
            "text", "policy-v1", "hash");
        SourceRef source = new SourceRef("doc-1", "file:///doc", 0, 4);
        GraphPathRef path = new GraphPathRef(Collections.singletonList("v1"),
            Collections.emptyList(), 0, false);
        Map<String, ChannelScore> bm25 = new HashMap<>();
        bm25.put("bm25", new ChannelScore("bm25", 2.0, 0.8, 1));
        Map<String, ChannelScore> vector = new HashMap<>();
        vector.put("vector", new ChannelScore("vector", 0.9, 0.7, 3));

        Evidence first = new Evidence("evidence-1", EvidenceKind.CHUNK, "text",
            Collections.singletonList(chunk), Collections.emptyList(),
            Collections.singletonList(path), Collections.singletonList(source), bm25, 0.8, 1);
        Evidence second = new Evidence("evidence-2", EvidenceKind.CHUNK, "text",
            Collections.singletonList(chunk), Collections.emptyList(),
            Collections.singletonList(path), Collections.singletonList(source), vector, 0.7, 3);

        Assertions.assertNotEquals(first, second);
        Assertions.assertTrue(first.sameIdentityAs(second));
        Assertions.assertEquals(0.8, first.getStageScores().get("bm25").getNormalizedScore());
        Assertions.assertEquals(0.7, second.getStageScores().get("vector").getNormalizedScore());
        String json = GSON.toJson(first);
        Assertions.assertTrue(json.contains("\"chunks\""));
        Assertions.assertTrue(json.contains("\"stageScores\""));
        Assertions.assertEquals(first, RetrievalModelJson.fromJson(json, Evidence.class));
    }

    @Test
    public void evidenceIdentityUsesNestedIdentityAndIgnoresCollectionOrder() {
        TextChunk firstChunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 1,
            "text", "policy-v1", "hash");
        TextChunk secondChunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 99,
            "text", "policy-v1", "hash");
        EntityRef firstEntity = new EntityRef("entity-1", "Name",
            Collections.singletonList("alias-a"), "PERSON", Collections.singletonList("chunk-1"));
        EntityRef secondEntity = new EntityRef("entity-1", "Name",
            Collections.singletonList("alias-b"), "PERSON", Collections.singletonList("chunk-2"));
        SourceRef source = new SourceRef("doc-1", "file:///doc", 0, 4);
        SourceRef secondSource = new SourceRef("doc-2", "file:///doc-2", 0, 4);
        GraphPathRef path = new GraphPathRef(Arrays.asList("v1", "v2"),
            Collections.singletonList("e1"), 1, false);
        GraphPathRef secondPath = new GraphPathRef(Arrays.asList("v2", "v3"),
            Collections.singletonList("e2"), 1, false);

        Evidence first = new Evidence("e-1", EvidenceKind.ENTITY, "first",
            Collections.singletonList(firstChunk), Collections.singletonList(firstEntity),
            Arrays.asList(path, secondPath), Arrays.asList(source, secondSource),
            Collections.<String, ChannelScore>emptyMap(), 0.1, 1);
        Evidence second = new Evidence("e-2", EvidenceKind.ENTITY, "second",
            Collections.singletonList(secondChunk), Collections.singletonList(secondEntity),
            Arrays.asList(secondPath, path), Arrays.asList(secondSource, source),
            Collections.<String, ChannelScore>emptyMap(), 0.9, 2);

        Assertions.assertTrue(first.sameIdentityAs(second));
        Evidence reordered = new Evidence("e-3", EvidenceKind.ENTITY, null,
            Collections.singletonList(secondChunk), Collections.singletonList(secondEntity),
            Arrays.asList(secondPath, path), Arrays.asList(secondSource, source),
            Collections.<String, ChannelScore>emptyMap(), null, null);
        Assertions.assertTrue(first.sameIdentityAs(reordered));
    }

    @Test
    public void gsonRoundTripPreservesFieldsAndOptionalCompatibility() throws IOException {
        SourceDocument document = new SourceDocument("doc-1", "hotpotqa", "v1",
            "dev", "Title", "https://example/doc-1", "sha256");
        String json = GSON.toJson(document);
        SourceDocument restored = RetrievalModelJson.fromJson(json, SourceDocument.class);
        Assertions.assertEquals(document, restored);
        Assertions.assertEquals(readFixture("retrieval/model/source-document.json"), json);

        String oldJson = "{\"documentId\":\"doc-1\",\"dataset\":\"hotpotqa\","
            + "\"datasetVersion\":\"v1\",\"split\":\"dev\","
            + "\"sourceUri\":\"https://example/doc-1\",\"sourceHash\":\"sha256\"}";
        SourceDocument withoutTitle = RetrievalModelJson.fromJson(oldJson, SourceDocument.class);
        Assertions.assertNull(withoutTitle.getTitle());
        Assertions.assertEquals(document.getDocumentId(), withoutTitle.getDocumentId());

        String oldEvidenceJson = "{\"kind\":\"CHUNK\",\"evidenceId\":\"e-1\","
            + "\"text\":\"text\"}";
        Evidence oldEvidence = RetrievalModelJson.fromJson(oldEvidenceJson, Evidence.class);
        Assertions.assertNotNull(oldEvidence.getChunks());
        Assertions.assertNotNull(oldEvidence.getEntities());
        Assertions.assertNotNull(oldEvidence.getPaths());
        Assertions.assertNotNull(oldEvidence.getSources());
        Assertions.assertNotNull(oldEvidence.getStageScores());
        Assertions.assertTrue(oldEvidence.getChunks().isEmpty());
        Assertions.assertTrue(GSON.toJson(oldEvidence).contains("\"kind\":\"CHUNK\""));
    }

    @Test
    public void validatedJsonRejectsMissingRequiredFieldsAndKeepsEmptyCollections() {
        Assertions.assertThrows(JsonParseException.class,
            () -> RetrievalModelJson.fromJson("{\"version\":\"v1\"}", GraphVersion.class));
        Assertions.assertThrows(JsonParseException.class,
            () -> RetrievalModelJson.fromJson("{\"kind\":\"CHUNK\",\"evidenceId\":\" \"}",
                Evidence.class));

        Evidence empty = RetrievalModelJson.fromJson("{\"kind\":\"CHUNK\","
            + "\"chunks\":null,\"entities\":[],\"paths\":null,\"sources\":null,"
            + "\"stageScores\":null}", Evidence.class);
        String json = RetrievalModelJson.toJson(empty);
        Assertions.assertTrue(json.contains("\"chunks\":[]"));
        Assertions.assertTrue(json.contains("\"entities\":[]"));
        Assertions.assertTrue(json.contains("\"paths\":[]"));
        Assertions.assertTrue(json.contains("\"sources\":[]"));
        Assertions.assertTrue(json.contains("\"stageScores\":{}"));
    }

    @Test
    public void validatedJsonRejectsWrongElementAndNumericTypes() {
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"entityId\":\"e\",\"canonicalName\":\"n\","
                + "\"aliases\":[1],\"type\":\"PERSON\",\"sourceChunkIds\":[]}",
            EntityRef.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"entityId\":\"e\",\"canonicalName\":\"n\","
                + "\"aliases\":[true],\"type\":\"PERSON\",\"sourceChunkIds\":[]}",
            EntityRef.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"entityId\":\"e\",\"canonicalName\":\"n\","
                + "\"aliases\":[null],\"type\":\"PERSON\",\"sourceChunkIds\":[]}",
            EntityRef.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"chunkId\":\"c\",\"documentId\":\"d\",\"chunkIndex\":1.9,"
                + "\"startOffset\":0,\"endOffset\":1,\"tokenEstimate\":1,\"text\":\"x\"}",
            TextChunk.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"channel\":\"bm25\",\"rawScore\":1,\"rank\":2.5}",
            ChannelScore.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"vertexIds\":[\"v1\",\"v2\"],\"edgeIds\":[\"e1\"],"
                + "\"hop\":1.5,\"sampled\":false}", GraphPathRef.class));
        Assertions.assertThrows(JsonParseException.class, () -> RetrievalModelJson.fromJson(
            "{\"kind\":\"CHUNK\",\"fusedScore\":1.1}", Evidence.class));
    }

    @Test
    public void modelValidationUsesDedicatedExceptionAndEnforcesRanges() {
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new Evidence("e", EvidenceKind.CHUNK, null, null, null, null, null,
                null, -0.01, null));
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new Evidence("e", EvidenceKind.CHUNK, null, null, null, null, null,
                null, 1.01, null));
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new GraphPathRef(Collections.singletonList("v1"),
                Collections.singletonList("e1"), 0, false));
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new GraphPathRef(Arrays.asList("v1", "v2"),
                Collections.emptyList(), 1, false));
    }

    @Test
    public void unknownJsonFieldsAreIgnoredAndIdentityDiffersFromValueEquality() {
        SourceDocument document = RetrievalModelJson.fromJson(
            "{\"documentId\":\"d\",\"dataset\":\"set\",\"datasetVersion\":\"v1\","
                + "\"split\":\"dev\",\"title\":\"title\",\"sourceUri\":\"uri\","
                + "\"sourceHash\":\"hash\",\"futureField\":true}", SourceDocument.class);
        SourceDocument changedTitle = new SourceDocument("d", "set", "v1", "dev",
            "other title", "uri", "hash");
        Assertions.assertTrue(document.sameIdentityAs(changedTitle));
        Assertions.assertNotEquals(document, changedTitle);
SourceDocument copy = new SourceDocument("d", "set", "v1", "dev", "title", "uri", "hash");
Assertions.assertEquals(document, copy);
Assertions.assertEquals(document.hashCode(), copy.hashCode());
    }

    @Test
    public void emptyEvidenceFallsBackToStableIdentityFields() {
        Evidence first = new Evidence("e1", EvidenceKind.CHUNK, null, null, null, null,
            null, null, null, null);
        Evidence second = new Evidence("e2", EvidenceKind.CHUNK, "different text", null,
            null, null, null, null, null, 1);
        Evidence otherKind = new Evidence("e3", EvidenceKind.ENTITY, null, null, null, null,
            null, null, null, null);
        Assertions.assertFalse(first.sameIdentityAs(second));
        Assertions.assertFalse(first.sameIdentityAs(otherKind));
        Assertions.assertNotEquals(first, second);

        Assertions.assertTrue(first.sameIdentityAs(new Evidence("e1", EvidenceKind.CHUNK,
            "different text", null, null, null, null, null, null, null)));
        Assertions.assertTrue(new Evidence(null, EvidenceKind.CHUNK, "same text", null, null,
            null, null, null, null, null).sameIdentityAs(new Evidence(null, EvidenceKind.CHUNK,
            "same text", null, null, null, null, null, null, null)));
        Assertions.assertFalse(new Evidence(null, EvidenceKind.CHUNK, null, null, null, null,
            null, null, null, null).sameIdentityAs(new Evidence(null, EvidenceKind.CHUNK, null,
            null, null, null, null, null, null, null)));
    }

    @Test
    public void constructorCopiesInputCollections() {
        java.util.List<String> aliases = new java.util.ArrayList<>();
        aliases.add("alias-a");
        EntityRef entity = new EntityRef("entity-1", "Name", aliases, "PERSON", aliases);
        aliases.set(0, "changed");
        Assertions.assertEquals(Collections.singletonList("alias-a"), entity.getAliases());
        Assertions.assertEquals(Collections.singletonList("alias-a"), entity.getSourceChunkIds());
    }

    @Test
    public void allModelsRoundTripThroughValidatedJson() {
        SourceDocument document = new SourceDocument("doc-1", "dataset", "v1", "dev",
            "Title", "file:///doc", "hash");
        TextChunk chunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 1,
            "text", "policy-v1", "hash");
        EntityRef entity = new EntityRef("entity-1", "Name", Collections.singletonList("N"),
            "PERSON", Collections.singletonList("chunk-1"));
        GraphVertexRef vertex = new GraphVertexRef("person", "vertex-1", "entity-1");
        GraphEdgeRef edge = new GraphEdgeRef("edge-1", "knows", "entity-1", "entity-2",
            Collections.singletonList("chunk-1"));
        GraphPathRef path = new GraphPathRef(Arrays.asList("vertex-1", "vertex-2"),
            Collections.singletonList("edge-1"), 1, false);
        SourceRef source = new SourceRef("doc-1", "file:///doc", 0, 4);
        ChannelScore score = new ChannelScore("bm25", 2.0, 0.8, 1);
        GraphVersion graphVersion = new GraphVersion("graph", "v1");
        IndexVersion indexVersion = new IndexVersion("bm25", "v1", "graph-v1");

        Assertions.assertEquals(document, roundTrip(document, SourceDocument.class));
        Assertions.assertEquals(chunk, roundTrip(chunk, TextChunk.class));
        Assertions.assertEquals(entity, roundTrip(entity, EntityRef.class));
        Assertions.assertEquals(vertex, roundTrip(vertex, GraphVertexRef.class));
        Assertions.assertEquals(edge, roundTrip(edge, GraphEdgeRef.class));
        Assertions.assertEquals(path, roundTrip(path, GraphPathRef.class));
        Assertions.assertEquals(source, roundTrip(source, SourceRef.class));
        Assertions.assertEquals(score, roundTrip(score, ChannelScore.class));
        Assertions.assertEquals(graphVersion, roundTrip(graphVersion, GraphVersion.class));
        Assertions.assertEquals(indexVersion, roundTrip(indexVersion, IndexVersion.class));
    }

    @Test
    public void completeEvidenceFixtureRoundTrips() throws IOException {
        String json = readFixture("retrieval/model/evidence.json");
        Evidence evidence = RetrievalModelJson.fromJson(json, Evidence.class);
        Assertions.assertEquals(evidence, RetrievalModelJson.fromJson(
            RetrievalModelJson.toJson(evidence), Evidence.class));
        Assertions.assertEquals(2, evidence.getStageScores().size());
        Assertions.assertEquals(1, evidence.getPaths().get(0).getHop());
    }

    @Test
    public void identityFixtureDocumentsFieldBasedComparisons() throws IOException {
        Map<String, Object> fixture = GSON.fromJson(readFixture(
            "retrieval/model/identity-cases.json"), Map.class);
        Assertions.assertNotNull(fixture.get("sourceDocument"));
        SourceDocument source = RetrievalModelJson.fromJson(GSON.toJson(fixture.get(
            "sourceDocument")), SourceDocument.class);
        SourceDocument sourceVariant = RetrievalModelJson.fromJson(GSON.toJson(fixture.get(
            "sourceDocumentVariant")), SourceDocument.class);
        Assertions.assertTrue(source.sameIdentityAs(sourceVariant));
        TextChunk chunk = RetrievalModelJson.fromJson(GSON.toJson(fixture.get("chunk")),
            TextChunk.class);
        TextChunk chunkVariant = RetrievalModelJson.fromJson(GSON.toJson(fixture.get(
            "chunkVariant")), TextChunk.class);
        Assertions.assertTrue(chunk.sameIdentityAs(chunkVariant));
        EntityRef entity = RetrievalModelJson.fromJson(GSON.toJson(fixture.get("entity")),
            EntityRef.class);
        EntityRef entityVariant = RetrievalModelJson.fromJson(GSON.toJson(fixture.get(
            "entityVariant")), EntityRef.class);
        Assertions.assertTrue(entity.sameIdentityAs(entityVariant));
        GraphEdgeRef edge = RetrievalModelJson.fromJson(GSON.toJson(fixture.get("edge")),
            GraphEdgeRef.class);
        GraphEdgeRef edgeVariant = RetrievalModelJson.fromJson(GSON.toJson(fixture.get(
            "edgeVariant")), GraphEdgeRef.class);
        Assertions.assertTrue(edge.sameIdentityAs(edgeVariant));
    }

    private <T> T roundTrip(T value, Class<T> type) {
        return RetrievalModelJson.fromJson(RetrievalModelJson.toJson(value), type);
    }

    private String readFixture(String name) throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(name);
        Assertions.assertNotNull(stream);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))) {
            return reader.lines().collect(Collectors.joining());
        }
    }
}
