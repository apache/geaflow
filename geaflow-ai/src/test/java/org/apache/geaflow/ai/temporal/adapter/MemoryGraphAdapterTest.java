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

package org.apache.geaflow.ai.temporal.adapter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Defines the canonical MemoryGraph projection and reverse-read contract.
 */
public class MemoryGraphAdapterTest {

    private static final String CITY_VERSION = "city-version";
    private static final String CITY_TOMBSTONE = "city-tombstone";
    private static final String BOB_VERSION = "bob-version";
    private static final String BOB_COPY_VERSION = "bob-copy-version";
    private static final String CAROL_VERSION = "carol-version";

    private static final String CITY_ADD_EVENT = "city-add";
    private static final String CITY_RETRACT_EVENT = "city-retract";
    private static final String BOB_ADD_EVENT = "bob-add";
    private static final String BOB_CORRECT_EVENT = "bob-correct";
    private static final String CAROL_ADD_EVENT = "carol-add";

    private static final String CITY_EVIDENCE = "city-proof";

    private static final FactKey CITY_KEY = new FactKey(
        "person:alice", "city", "profile");
    private static final FactKey KNOWS_KEY = new FactKey(
        "person:alice", "knows", "social");
    private static final FactKey ALIAS_KEY = new FactKey(
        "person:history", "alias", "profile");

    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testEmptySnapshotCreatesFixedSchemaAndRoundTrips() {
        CanonicalSnapshot empty = new CanonicalSnapshot(
            new TemporalState(
                Collections.emptyMap(),
                Collections.emptyList()),
            Collections.emptyList(),
            Collections.emptyMap());

        MemoryGraph graph = new MemoryGraphAdapter().toGraph(empty);

        Assertions.assertEquals(expectedVertexSchemas(), vertexSchemas(graph));
        Assertions.assertEquals(expectedEdgeSchemas(), edgeSchemas(graph));
        Assertions.assertEquals(expectedGroupOrder(),
            new ArrayList<>(graph.entities.keySet()));
        for (EntityGroup group : graph.entities.values()) {
            if (group instanceof VertexGroup) {
                Assertions.assertTrue(
                    ((VertexGroup) group).getVertices().isEmpty());
            } else {
                Assertions.assertTrue(
                    ((EdgeGroup) group).getOutEdges().isEmpty());
            }
        }
        Assertions.assertEquals(
            empty,
            new MemoryGraphAdapter().fromGraph(graph));
    }

    @Test
    public void testCompleteSnapshotRoundTripsWithoutLoss() {
        CanonicalSnapshot expected = completeSnapshot();
        MemoryGraph graph = new MemoryGraphAdapter().toGraph(expected);

        CanonicalSnapshot actual =
            new MemoryGraphAdapter().fromGraph(graph);

        Assertions.assertEquals(expected, actual);
        Assertions.assertNotNull(graph.getVertex(
            "entity", "entity:person:alice"));
        Assertions.assertNotNull(graph.getVertex(
            "entity", "entity:person:bob"));
        Assertions.assertNotNull(graph.getVertex(
            "entity", "entity:person:carol"));
        Assertions.assertNotNull(graph.getVertex(
            "entity", "entity:person:history"));
        Assertions.assertNotNull(graph.getVertex(
            "fact_version", versionId(CITY_TOMBSTONE)));
        Assertions.assertNotNull(graph.getVertex(
            "memory_event", eventId(CITY_RETRACT_EVENT)));
        Assertions.assertNotNull(graph.getVertex(
            "evidence", evidenceId(CITY_EVIDENCE)));
        Assertions.assertNotNull(graph.getVertex(
            "source", "source:registry"));
        Assertions.assertEquals(
            event(expected, CITY_ADD_EVENT).getPayloadHash(),
            vertexProperty(
                graph,
                "memory_event",
                eventId(CITY_ADD_EVENT),
                "payloadHash"));

        Assertions.assertTrue(outEdges(
            graph, "object", versionId(CITY_VERSION)).isEmpty());
        assertSingleEdge(
            graph,
            "subject",
            versionId(CITY_VERSION),
            "entity:person:alice");
        assertSingleEdge(
            graph,
            "object",
            versionId(BOB_VERSION),
            "entity:person:bob");
        assertSingleEdge(
            graph,
            "generates",
            eventId(CITY_ADD_EVENT),
            versionId(CITY_VERSION));
        assertSingleEdge(
            graph,
            "from_source",
            evidenceId(CITY_EVIDENCE),
            "source:registry");

        Assertions.assertEquals(
            Collections.singletonList("2"),
            assertSingleEdge(
                graph,
                "supported_by",
                eventId(CITY_ADD_EVENT),
                evidenceId(CITY_EVIDENCE)).getValues());
        Assertions.assertEquals(
            Collections.singletonList("2"),
            assertSingleEdge(
                graph,
                "supported_by",
                versionId(CITY_VERSION),
                evidenceId(CITY_EVIDENCE)).getValues());

        assertRelationEdge(
            graph,
            "supersedes",
            versionId(CITY_TOMBSTONE),
            versionId(CITY_VERSION),
            1);
        assertRelationEdge(
            graph,
            "duplicate_of",
            versionId(BOB_COPY_VERSION),
            versionId(BOB_VERSION),
            2);
        assertRelationEdge(
            graph,
            "conflicts_with",
            versionId(BOB_VERSION),
            versionId(CAROL_VERSION),
            1);
    }

    @Test
    public void testProjectionIsDeterministicAndUsesCanonicalEdges() {
        CanonicalSnapshot snapshot = completeSnapshot();
        MemoryGraph first = new MemoryGraphAdapter().toGraph(snapshot);
        MemoryGraph second = new MemoryGraphAdapter().toGraph(snapshot);

        Assertions.assertEquals(graphRows(first), graphRows(second));
        Assertions.assertEquals(
            graphRows(first),
            graphRows(new MemoryGraphAdapter().toGraph(
                new MemoryGraphAdapter().fromGraph(first))));

        Set<Edge> identities = new HashSet<>();
        Set<String> relationIds = new HashSet<>();
        for (VertexSchema schema
            : first.getGraphSchema().getVertexSchemaList()) {
            for (Vertex vertex : vertices(first, schema.getLabel())) {
                Assertions.assertEquals(
                    schema.getFields().size(),
                    vertex.getValues().size());
            }
        }
        for (EdgeSchema schema
            : first.getGraphSchema().getEdgeSchemaList()) {
            for (Edge edge : edges(first, schema.getLabel())) {
                Assertions.assertEquals(
                    schema.getFields().size(),
                    edge.getValues().size());
                Assertions.assertTrue(
                    identities.add(edge),
                    "duplicate edge identity: " + edge);
                if (isVersionRelation(edge.getLabel())) {
                    Assertions.assertTrue(
                        relationIds.add(edge.getValues().get(0)));
                }
            }
        }
        Assertions.assertEquals(3, relationIds.size());
        assertTypedVertexIds(first);
    }

    @Test
    public void testNullInputsAreRejected() {
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();

        Assertions.assertThrows(
            NullPointerException.class,
            () -> adapter.toGraph(null));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> adapter.fromGraph(null));
    }

    @Test
    public void testOrphanVerticesAreRejected() {
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();

        MemoryGraph entityGraph = emptyGraph(adapter);
        entityGraph.addVertex(new Vertex(
            "entity",
            "entity:orphan",
            Collections.singletonList("person")));
        IllegalArgumentException entityError = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> adapter.fromGraph(entityGraph));
        Assertions.assertEquals(
            "Orphan entity vertex: entity:orphan",
            entityError.getMessage());

        MemoryGraph evidenceGraph = emptyGraph(adapter);
        evidenceGraph.addVertex(new Vertex(
            "source",
            "source:orphan-evidence-source",
            Collections.singletonList("Orphan evidence source")));
        evidenceGraph.addVertex(new Vertex(
            "evidence",
            "evidence:orphan",
            Collections.singletonList("Unreferenced evidence")));
        evidenceGraph.addEdge(new Edge(
            "from_source",
            "evidence:orphan",
            "source:orphan-evidence-source",
            Collections.emptyList()));
        IllegalArgumentException evidenceError = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> adapter.fromGraph(evidenceGraph));
        Assertions.assertEquals(
            "Orphan evidence vertex: evidence:orphan",
            evidenceError.getMessage());

        MemoryGraph sourceGraph = emptyGraph(adapter);
        sourceGraph.addVertex(new Vertex(
            "source",
            "source:orphan",
            Collections.singletonList("Unreferenced source")));
        IllegalArgumentException sourceError = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> adapter.fromGraph(sourceGraph));
        Assertions.assertEquals(
            "Orphan source vertex: source:orphan",
            sourceError.getMessage());
    }

    private static MemoryGraph emptyGraph(MemoryGraphAdapter adapter) {
        return adapter.toGraph(new CanonicalSnapshot(
            new TemporalState(
                Collections.emptyMap(),
                Collections.emptyList()),
            Collections.emptyList(),
            Collections.emptyMap()));
    }

    private CanonicalSnapshot completeSnapshot() {
        Source registry = new Source("registry", "Registry");
        Source analyst = new Source("analyst", "Analyst notes");
        Evidence cityEvidence = new Evidence(
            CITY_EVIDENCE, registry, "Alice lived in Beijing");
        Evidence retractEvidence = new Evidence(
            "retract-proof", registry, "The city record expired");
        Evidence bobEvidence = new Evidence(
            "bob-proof", analyst, "Alice knows Bob");
        Evidence bobCopyEvidence = new Evidence(
            "bob-copy-proof", analyst, "Duplicate Bob assertion");
        Evidence carolEvidence = new Evidence(
            "carol-proof", registry, "Alice also knows Carol");
        Evidence aliasEvidence = new Evidence(
            "alias-proof", analyst, "Historical alias event");

        MemoryEntity alice = new MemoryEntity("person:alice", "person");
        MemoryEntity bob = new MemoryEntity("person:bob", "person");
        MemoryEntity carol = new MemoryEntity("person:carol", "person");
        MemoryEntity history = new MemoryEntity(
            "person:history", "historical_person");
        MemoryFact cityFact = MemoryFact.attribute(
            "fact-city", alice, "city", "Beijing");
        MemoryFact bobFact = MemoryFact.relationship(
            "fact-bob", alice, "knows", bob);
        MemoryFact bobCopyFact = MemoryFact.relationship(
            "fact-bob-copy", alice, "knows", bob);
        MemoryFact carolFact = MemoryFact.relationship(
            "fact-carol", alice, "knows", carol);
        MemoryFact aliasFact = MemoryFact.attribute(
            "fact-alias", history, "alias", "Al");

        TimeInterval cityValid = interval(
            "2020-01-01T00:00:00Z",
            "2021-01-01T00:00:00Z");
        TimeInterval relationshipValid = TimeInterval.unboundedFrom(
            Instant.parse("2019-06-01T00:00:00Z"));
        NormalizedMemoryEvent cityAdd = add(
            CITY_ADD_EVENT,
            cityFact,
            CITY_KEY,
            cityValid,
            "2025-01-01T00:00:00Z",
            cityEvidence,
            cityEvidence);
        NormalizedMemoryEvent cityRetract = retract(
            CITY_RETRACT_EVENT,
            cityFact.getId(),
            CITY_KEY,
            cityValid,
            "2025-01-02T00:00:00Z",
            retractEvidence);
        NormalizedMemoryEvent bobAdd = add(
            BOB_ADD_EVENT,
            bobFact,
            KNOWS_KEY,
            relationshipValid,
            "2025-01-03T00:00:00Z",
            bobEvidence);
        NormalizedMemoryEvent bobCorrect = correct(
            BOB_CORRECT_EVENT,
            bobCopyFact,
            KNOWS_KEY,
            relationshipValid,
            "2025-01-04T00:00:00Z",
            bobCopyEvidence);
        NormalizedMemoryEvent carolAdd = add(
            CAROL_ADD_EVENT,
            carolFact,
            KNOWS_KEY,
            relationshipValid,
            "2025-01-05T00:00:00Z",
            carolEvidence);
        NormalizedMemoryEvent historicalAlias = add(
            "alias-history",
            aliasFact,
            ALIAS_KEY,
            TimeInterval.unboundedFrom(
                Instant.parse("2018-01-01T00:00:00Z")),
            "2025-01-06T00:00:00Z",
            aliasEvidence);

        MemoryFactVersion cityVersion = version(
            CITY_VERSION,
            cityFact,
            MemoryFactVersionStatus.ACTIVE,
            interval(
                "2020-01-01T00:00:00.123456789Z",
                "2021-01-01T00:00:00.987654321Z"),
            interval(
                "2025-01-01T00:00:00.000000001Z",
                "2025-01-02T00:00:00.000000002Z"),
            cityEvidence,
            cityEvidence);
        MemoryFactVersion cityTombstone = version(
            CITY_TOMBSTONE,
            cityFact,
            MemoryFactVersionStatus.RETRACTED,
            cityValid,
            TimeInterval.unboundedFrom(
                Instant.parse("2025-01-02T00:00:00Z")),
            retractEvidence);
        MemoryFactVersion bobVersion = version(
            BOB_VERSION,
            bobFact,
            MemoryFactVersionStatus.ACTIVE,
            relationshipValid,
            interval(
                "2025-01-03T00:00:00Z",
                "2025-01-04T00:00:00Z"),
            bobEvidence);
        MemoryFactVersion bobCopyVersion = version(
            BOB_COPY_VERSION,
            bobCopyFact,
            MemoryFactVersionStatus.ACTIVE,
            relationshipValid,
            interval(
                "2025-01-04T00:00:00Z",
                "2025-01-05T00:00:00Z"),
            bobCopyEvidence);
        MemoryFactVersion carolVersion = version(
            CAROL_VERSION,
            carolFact,
            MemoryFactVersionStatus.ACTIVE,
            relationshipValid,
            TimeInterval.unboundedFrom(
                Instant.parse("2025-01-05T00:00:00Z")),
            carolEvidence);

        Map<FactKey, List<MemoryFactVersion>> versions =
            new LinkedHashMap<>();
        versions.put(KNOWS_KEY, Arrays.asList(
            carolVersion, bobCopyVersion, bobVersion));
        versions.put(CITY_KEY, Arrays.asList(
            cityTombstone, cityVersion));

        VersionRelation duplicate = new VersionRelation(
            VersionRelationType.DUPLICATE_OF,
            BOB_COPY_VERSION,
            BOB_VERSION);
        List<VersionRelation> relations = Arrays.asList(
            new VersionRelation(
                VersionRelationType.CONFLICTS_WITH,
                BOB_VERSION,
                CAROL_VERSION),
            duplicate,
            new VersionRelation(
                VersionRelationType.SUPERSEDES,
                CITY_TOMBSTONE,
                CITY_VERSION),
            duplicate);

        Map<String, String> generating = new LinkedHashMap<>();
        generating.put(CAROL_VERSION, CAROL_ADD_EVENT);
        generating.put(CITY_TOMBSTONE, CITY_RETRACT_EVENT);
        generating.put(BOB_COPY_VERSION, BOB_CORRECT_EVENT);
        generating.put(CITY_VERSION, CITY_ADD_EVENT);
        generating.put(BOB_VERSION, BOB_ADD_EVENT);

        return new CanonicalSnapshot(
            new TemporalState(versions, relations),
            Arrays.asList(
                historicalAlias,
                carolAdd,
                bobCorrect,
                bobAdd,
                cityRetract,
                cityAdd),
            generating);
    }

    private NormalizedMemoryEvent add(
        String eventId,
        MemoryFact fact,
        FactKey key,
        TimeInterval validTime,
        String recordedAt,
        Evidence... evidence) {
        return normalizer.normalize(
            MemoryEvent.add(
                eventId,
                fact,
                validTime,
                Instant.parse(recordedAt),
                Arrays.asList(evidence)),
            key);
    }

    private NormalizedMemoryEvent correct(
        String eventId,
        MemoryFact fact,
        FactKey key,
        TimeInterval validTime,
        String recordedAt,
        Evidence... evidence) {
        return normalizer.normalize(
            MemoryEvent.correct(
                eventId,
                fact,
                validTime,
                Instant.parse(recordedAt),
                Arrays.asList(evidence)),
            key);
    }

    private NormalizedMemoryEvent retract(
        String eventId,
        String factId,
        FactKey key,
        TimeInterval validTime,
        String recordedAt,
        Evidence... evidence) {
        return normalizer.normalize(
            MemoryEvent.retract(
                eventId,
                factId,
                validTime,
                Instant.parse(recordedAt),
                Arrays.asList(evidence)),
            key);
    }

    private static MemoryFactVersion version(
        String versionId,
        MemoryFact fact,
        MemoryFactVersionStatus status,
        TimeInterval validTime,
        TimeInterval transactionTime,
        Evidence... evidence) {
        return new MemoryFactVersion(
            versionId,
            fact,
            status,
            validTime,
            transactionTime,
            Arrays.asList(evidence));
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(Instant.parse(start), Instant.parse(end));
    }

    private static List<String> expectedVertexSchemas() {
        return Arrays.asList(
            "entity|id|[label]",
            "fact_version|id|[factId, predicate, scope, valueKind, "
                + "literalValue, status, validStart, validEnd, "
                + "transactionStart, transactionEnd]",
            "memory_event|id|[operation, factId, subjectId, predicate, "
                + "scope, valueKind, value, validStart, validEnd, "
                + "recordedAt, payloadHash]",
            "evidence|id|[content]",
            "source|id|[name]");
    }

    private static List<String> expectedEdgeSchemas() {
        return Arrays.asList(
            "subject|srcId|dstId|[]",
            "object|srcId|dstId|[]",
            "generates|srcId|dstId|[]",
            "supported_by|srcId|dstId|[occurrenceCount]",
            "from_source|srcId|dstId|[]",
            "supersedes|srcId|dstId|[relationId, occurrenceCount]",
            "duplicate_of|srcId|dstId|[relationId, occurrenceCount]",
            "conflicts_with|srcId|dstId|[relationId, occurrenceCount]");
    }

    private static List<String> expectedGroupOrder() {
        return Arrays.asList(
            "entity",
            "fact_version",
            "memory_event",
            "evidence",
            "source",
            "subject",
            "object",
            "generates",
            "supported_by",
            "from_source",
            "supersedes",
            "duplicate_of",
            "conflicts_with");
    }

    private static List<String> vertexSchemas(MemoryGraph graph) {
        List<String> rows = new ArrayList<>();
        for (VertexSchema schema
            : graph.getGraphSchema().getVertexSchemaList()) {
            rows.add(schema.getLabel() + "|" + schema.getIdField()
                + "|" + schema.getFields());
        }
        return rows;
    }

    private static List<String> edgeSchemas(MemoryGraph graph) {
        List<String> rows = new ArrayList<>();
        for (EdgeSchema schema
            : graph.getGraphSchema().getEdgeSchemaList()) {
            rows.add(schema.getLabel() + "|" + schema.getSrcIdField()
                + "|" + schema.getDstIdField()
                + "|" + schema.getFields());
        }
        return rows;
    }

    private static List<String> graphRows(MemoryGraph graph) {
        List<String> rows = new ArrayList<>();
        rows.addAll(vertexSchemas(graph));
        rows.addAll(edgeSchemas(graph));
        for (VertexSchema schema
            : graph.getGraphSchema().getVertexSchemaList()) {
            for (Vertex vertex : vertices(graph, schema.getLabel())) {
                rows.add("vertex|" + vertex.getLabel() + "|"
                    + vertex.getId() + "|" + vertex.getValues());
            }
        }
        for (EdgeSchema schema
            : graph.getGraphSchema().getEdgeSchemaList()) {
            for (Edge edge : edges(graph, schema.getLabel())) {
                rows.add("edge|" + edge.getLabel() + "|"
                    + edge.getSrcId() + "|" + edge.getDstId()
                    + "|" + edge.getValues());
            }
        }
        return rows;
    }

    private static List<Vertex> vertices(MemoryGraph graph, String label) {
        return ((VertexGroup) graph.entities.get(label)).getVertices();
    }

    private static String vertexProperty(
        MemoryGraph graph,
        String label,
        String vertexId,
        String field) {
        Vertex vertex = graph.getVertex(label, vertexId);
        Assertions.assertNotNull(vertex);
        for (VertexSchema schema
            : graph.getGraphSchema().getVertexSchemaList()) {
            if (label.equals(schema.getLabel())) {
                int index = schema.getFields().indexOf(field);
                Assertions.assertTrue(index >= 0);
                return vertex.getValues().get(index);
            }
        }
        throw new AssertionError("Missing vertex schema: " + label);
    }

    private static List<Edge> edges(MemoryGraph graph, String label) {
        return ((EdgeGroup) graph.entities.get(label)).getOutEdges();
    }

    private static List<Edge> outEdges(
        MemoryGraph graph,
        String label,
        String sourceId) {
        return ((EdgeGroup) graph.entities.get(label)).getOutEdges(sourceId);
    }

    private static Edge assertSingleEdge(
        MemoryGraph graph,
        String label,
        String sourceId,
        String targetId) {
        List<Edge> matches = graph.getEdge(label, sourceId, targetId);
        Assertions.assertEquals(1, matches.size());
        return matches.get(0);
    }

    private static void assertRelationEdge(
        MemoryGraph graph,
        String label,
        String sourceId,
        String targetId,
        int occurrenceCount) {
        List<String> values = assertSingleEdge(
            graph, label, sourceId, targetId).getValues();
        Assertions.assertEquals(2, values.size());
        Assertions.assertFalse(values.get(0).trim().isEmpty());
        Assertions.assertEquals(
            Integer.toString(occurrenceCount), values.get(1));
    }

    private static boolean isVersionRelation(String label) {
        return "supersedes".equals(label)
            || "duplicate_of".equals(label)
            || "conflicts_with".equals(label);
    }

    private static void assertTypedVertexIds(MemoryGraph graph) {
        Map<String, String> prefixes = new LinkedHashMap<>();
        prefixes.put("entity", "entity:");
        prefixes.put("fact_version", "version:");
        prefixes.put("memory_event", "event:");
        prefixes.put("evidence", "evidence:");
        prefixes.put("source", "source:");
        for (Map.Entry<String, String> entry : prefixes.entrySet()) {
            for (Vertex vertex : vertices(graph, entry.getKey())) {
                Assertions.assertTrue(
                    vertex.getId().startsWith(entry.getValue()));
            }
        }
    }

    private static String versionId(String rawId) {
        return "version:" + rawId;
    }

    private static String eventId(String rawId) {
        return "event:" + rawId;
    }

    private static String evidenceId(String rawId) {
        return "evidence:" + rawId;
    }

    private static NormalizedMemoryEvent event(
        CanonicalSnapshot snapshot,
        String eventId) {
        for (NormalizedMemoryEvent event : snapshot.getEvents()) {
            if (eventId.equals(event.getEventId())) {
                return event;
            }
        }
        throw new AssertionError("Missing event: " + eventId);
    }
}
