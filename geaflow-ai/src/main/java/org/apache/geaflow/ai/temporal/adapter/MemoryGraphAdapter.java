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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
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

/**
 * Projects canonical temporal snapshots to the existing in-memory graph model.
 */
public final class MemoryGraphAdapter {

    private static final String ENTITY = "entity";
    private static final String FACT_VERSION = "fact_version";
    private static final String MEMORY_EVENT = "memory_event";
    private static final String EVIDENCE = "evidence";
    private static final String SOURCE = "source";

    private static final String SUBJECT = "subject";
    private static final String OBJECT = "object";
    private static final String GENERATES = "generates";
    private static final String SUPPORTED_BY = "supported_by";
    private static final String FROM_SOURCE = "from_source";
    private static final String SUPERSEDES = "supersedes";
    private static final String DUPLICATE_OF = "duplicate_of";
    private static final String CONFLICTS_WITH = "conflicts_with";

    private static final String ENTITY_PREFIX = "entity:";
    private static final String VERSION_PREFIX = "version:";
    private static final String EVENT_PREFIX = "event:";
    private static final String EVIDENCE_PREFIX = "evidence:";
    private static final String SOURCE_PREFIX = "source:";
    private static final String EMPTY = "";

    private static final List<String> ENTITY_FIELDS = fields("label");
    private static final List<String> VERSION_FIELDS = fields(
        "factId",
        "predicate",
        "scope",
        "valueKind",
        "literalValue",
        "status",
        "validStart",
        "validEnd",
        "transactionStart",
        "transactionEnd");
    private static final List<String> EVENT_FIELDS = fields(
        "operation",
        "factId",
        "subjectId",
        "predicate",
        "scope",
        "valueKind",
        "value",
        "validStart",
        "validEnd",
        "recordedAt",
        "payloadHash");
    private static final List<String> EVIDENCE_FIELDS = fields("content");
    private static final List<String> SOURCE_FIELDS = fields("name");
    private static final List<String> NO_FIELDS = Collections.emptyList();
    private static final List<String> COUNT_FIELDS = fields(
        "occurrenceCount");
    private static final List<String> RELATION_FIELDS = fields(
        "relationId", "occurrenceCount");

    private static final List<String> VERTEX_LABELS = fields(
        ENTITY, FACT_VERSION, MEMORY_EVENT, EVIDENCE, SOURCE);
    private static final List<String> EDGE_LABELS = fields(
        SUBJECT,
        OBJECT,
        GENERATES,
        SUPPORTED_BY,
        FROM_SOURCE,
        SUPERSEDES,
        DUPLICATE_OF,
        CONFLICTS_WITH);

    private static final Comparator<Vertex> VERTEX_ORDER =
        Comparator.comparing(Vertex::getId);
    private static final Comparator<Edge> EDGE_ORDER =
        Comparator.comparing(Edge::getSrcId)
            .thenComparing(Edge::getDstId)
            .thenComparing(edge -> edge.getValues().toString());

    public MemoryGraph toGraph(CanonicalSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");

        Map<String, MemoryEntity> memoryEntities = new TreeMap<>();
        Map<String, Evidence> evidenceById = new TreeMap<>();
        Map<String, Source> sources = new TreeMap<>();
        Map<String, Vertex> versionVertices = new TreeMap<>();
        Map<String, Vertex> eventVertices = new TreeMap<>();
        Map<String, List<Edge>> edges = emptyEdgeLists();
        Map<String, CountedEdge> supported = new TreeMap<>();
        Map<String, Map<String, CountedEdge>> relationEdges =
            relationEdgeMaps();

        for (NormalizedMemoryEvent event : snapshot.getEvents()) {
            collectEventEntities(event, memoryEntities);
            collectEvidence(event.getEvidence(), evidenceById, sources);
            String eventGraphId = eventId(event.getEventId());
            putUniqueVertex(
                eventVertices,
                new Vertex(MEMORY_EVENT, eventGraphId, eventValues(event)));
            for (Evidence evidence : event.getEvidence()) {
                addCountedEdge(
                    supported,
                    eventGraphId,
                    evidenceId(evidence.getId()));
            }
        }

        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : snapshot.getState().getVersionsByFactKey().entrySet()) {
            FactKey key = entry.getKey();
            for (MemoryFactVersion version : entry.getValue()) {
                MemoryFact fact = version.getFact();
                collectEntity(memoryEntities, fact.getSubject());
                if (fact.isRelationship()) {
                    collectEntity(memoryEntities, fact.getTarget().get());
                }
                collectEvidence(version.getEvidence(), evidenceById, sources);

                String versionGraphId = versionId(version.getId());
                putUniqueVertex(
                    versionVertices,
                    new Vertex(
                        FACT_VERSION,
                        versionGraphId,
                        versionValues(version, key)));
                edges.get(SUBJECT).add(new Edge(
                    SUBJECT,
                    versionGraphId,
                    entityId(fact.getSubject().getId()),
                    NO_FIELDS));
                if (fact.isRelationship()) {
                    edges.get(OBJECT).add(new Edge(
                        OBJECT,
                        versionGraphId,
                        entityId(fact.getTarget().get().getId()),
                        NO_FIELDS));
                }
                String generatingEventId =
                    snapshot.getGeneratingEventIds().get(version.getId());
                edges.get(GENERATES).add(new Edge(
                    GENERATES,
                    eventId(generatingEventId),
                    versionGraphId,
                    NO_FIELDS));
                for (Evidence evidence : version.getEvidence()) {
                    addCountedEdge(
                        supported,
                        versionGraphId,
                        evidenceId(evidence.getId()));
                }
            }
        }

        for (VersionRelation relation
            : snapshot.getState().getRelations()) {
            String label = relationLabel(relation.getType());
            addCountedEdge(
                relationEdges.get(label),
                versionId(relation.getFromVersionId()),
                versionId(relation.getToVersionId()));
        }

        List<Vertex> entityVertices = new ArrayList<>();
        for (MemoryEntity entity : memoryEntities.values()) {
            entityVertices.add(new Vertex(
                ENTITY,
                entityId(entity.getId()),
                fields(entity.getLabel())));
        }
        List<Vertex> evidenceVertices = new ArrayList<>();
        for (Evidence evidence : evidenceById.values()) {
            evidenceVertices.add(new Vertex(
                EVIDENCE,
                evidenceId(evidence.getId()),
                fields(evidence.getContent())));
            edges.get(FROM_SOURCE).add(new Edge(
                FROM_SOURCE,
                evidenceId(evidence.getId()),
                sourceId(evidence.getSource().getId()),
                NO_FIELDS));
        }
        List<Vertex> sourceVertices = new ArrayList<>();
        for (Source source : sources.values()) {
            sourceVertices.add(new Vertex(
                SOURCE,
                sourceId(source.getId()),
                fields(source.getName())));
        }

        edges.put(
            SUPPORTED_BY,
            countedEdges(SUPPORTED_BY, supported, false));
        for (String label : Arrays.asList(
            SUPERSEDES, DUPLICATE_OF, CONFLICTS_WITH)) {
            edges.put(
                label,
                countedEdges(label, relationEdges.get(label), true));
        }

        Map<String, List<Vertex>> vertices = new LinkedHashMap<>();
        vertices.put(ENTITY, entityVertices);
        vertices.put(
            FACT_VERSION,
            new ArrayList<>(versionVertices.values()));
        vertices.put(
            MEMORY_EVENT,
            new ArrayList<>(eventVertices.values()));
        vertices.put(EVIDENCE, evidenceVertices);
        vertices.put(SOURCE, sourceVertices);
        return createGraph(vertices, edges);
    }

    public CanonicalSnapshot fromGraph(MemoryGraph graph) {
        Objects.requireNonNull(graph, "graph");
        validateGraphShape(graph);

        Map<String, Vertex> entityVertices =
            vertexIndex(graph, ENTITY, ENTITY_PREFIX, ENTITY_FIELDS);
        Map<String, Vertex> versionVertices =
            vertexIndex(
                graph,
                FACT_VERSION,
                VERSION_PREFIX,
                VERSION_FIELDS);
        Map<String, Vertex> eventVertices =
            vertexIndex(
                graph,
                MEMORY_EVENT,
                EVENT_PREFIX,
                EVENT_FIELDS);
        Map<String, Vertex> evidenceVertices =
            vertexIndex(
                graph,
                EVIDENCE,
                EVIDENCE_PREFIX,
                EVIDENCE_FIELDS);
        Map<String, Vertex> sourceVertices =
            vertexIndex(graph, SOURCE, SOURCE_PREFIX, SOURCE_FIELDS);
        Map<String, List<Edge>> edges = edgeIndex(graph);

        Map<String, MemoryEntity> memoryEntities =
            readEntities(entityVertices);
        Map<String, Source> sources = readSources(sourceVertices);
        Map<String, Evidence> evidence = readEvidence(
            evidenceVertices,
            sources,
            edges.get(FROM_SOURCE));
        Map<String, List<Evidence>> evidenceByOwner =
            readSupportedEvidence(
                edges.get(SUPPORTED_BY),
                eventVertices,
                versionVertices,
                evidence);

        Map<String, NormalizedMemoryEvent> events = readEvents(
            eventVertices,
            memoryEntities,
            evidenceByOwner);
        Map<FactKey, List<MemoryFactVersion>> versions = readVersions(
            versionVertices,
            memoryEntities,
            evidenceByOwner,
            edges.get(SUBJECT),
            edges.get(OBJECT));
        validateNoOrphanVertices(
            memoryEntities,
            evidence,
            sources,
            events,
            versions);
        Map<String, String> generating = readGeneratingEvents(
            edges.get(GENERATES),
            events,
            versionVertices);
        List<VersionRelation> relations = readRelations(
            edges,
            versionVertices);

        return new CanonicalSnapshot(
            new TemporalState(versions, relations),
            new ArrayList<>(events.values()),
            generating);
    }

    private static MemoryGraph createGraph(
        Map<String, List<Vertex>> vertices,
        Map<String, List<Edge>> edges) {
        GraphSchema schema = new GraphSchema();
        Map<String, VertexSchema> vertexSchemas = new LinkedHashMap<>();
        addVertexSchema(schema, vertexSchemas, ENTITY, ENTITY_FIELDS);
        addVertexSchema(
            schema, vertexSchemas, FACT_VERSION, VERSION_FIELDS);
        addVertexSchema(
            schema, vertexSchemas, MEMORY_EVENT, EVENT_FIELDS);
        addVertexSchema(schema, vertexSchemas, EVIDENCE, EVIDENCE_FIELDS);
        addVertexSchema(schema, vertexSchemas, SOURCE, SOURCE_FIELDS);

        Map<String, EdgeSchema> edgeSchemas = new LinkedHashMap<>();
        addEdgeSchema(schema, edgeSchemas, SUBJECT, NO_FIELDS);
        addEdgeSchema(schema, edgeSchemas, OBJECT, NO_FIELDS);
        addEdgeSchema(schema, edgeSchemas, GENERATES, NO_FIELDS);
        addEdgeSchema(schema, edgeSchemas, SUPPORTED_BY, COUNT_FIELDS);
        addEdgeSchema(schema, edgeSchemas, FROM_SOURCE, NO_FIELDS);
        addEdgeSchema(schema, edgeSchemas, SUPERSEDES, RELATION_FIELDS);
        addEdgeSchema(schema, edgeSchemas, DUPLICATE_OF, RELATION_FIELDS);
        addEdgeSchema(
            schema, edgeSchemas, CONFLICTS_WITH, RELATION_FIELDS);

        Map<String, EntityGroup> groups = new LinkedHashMap<>();
        for (String label : VERTEX_LABELS) {
            List<Vertex> rows = vertices.get(label);
            Collections.sort(rows, VERTEX_ORDER);
            groups.put(
                label,
                new VertexGroup(vertexSchemas.get(label), rows));
        }
        for (String label : EDGE_LABELS) {
            List<Edge> rows = edges.get(label);
            Collections.sort(rows, EDGE_ORDER);
            groups.put(
                label,
                new EdgeGroup(edgeSchemas.get(label), rows));
        }
        return new MemoryGraph(schema, groups);
    }

    private static void addVertexSchema(
        GraphSchema graphSchema,
        Map<String, VertexSchema> schemas,
        String label,
        List<String> fields) {
        VertexSchema schema = new VertexSchema(label, "id", fields);
        graphSchema.addVertex(schema);
        schemas.put(label, schema);
    }

    private static void addEdgeSchema(
        GraphSchema graphSchema,
        Map<String, EdgeSchema> schemas,
        String label,
        List<String> fields) {
        EdgeSchema schema = new EdgeSchema(
            label, "srcId", "dstId", fields);
        graphSchema.addEdge(schema);
        schemas.put(label, schema);
    }

    private static Map<String, List<Edge>> emptyEdgeLists() {
        Map<String, List<Edge>> edges = new LinkedHashMap<>();
        for (String label : EDGE_LABELS) {
            edges.put(label, new ArrayList<>());
        }
        return edges;
    }

    private static Map<String, Map<String, CountedEdge>>
        relationEdgeMaps() {
        Map<String, Map<String, CountedEdge>> relations =
            new LinkedHashMap<>();
        relations.put(SUPERSEDES, new TreeMap<>());
        relations.put(DUPLICATE_OF, new TreeMap<>());
        relations.put(CONFLICTS_WITH, new TreeMap<>());
        return relations;
    }

    private static List<Edge> countedEdges(
        String label,
        Map<String, CountedEdge> counted,
        boolean relation) {
        List<Edge> edges = new ArrayList<>();
        for (CountedEdge item : counted.values()) {
            List<String> values = relation
                ? fields(
                    relationId(label, item.sourceId, item.targetId),
                    Integer.toString(item.count))
                : fields(Integer.toString(item.count));
            edges.add(new Edge(
                label, item.sourceId, item.targetId, values));
        }
        return edges;
    }

    private static void addCountedEdge(
        Map<String, CountedEdge> edges,
        String sourceId,
        String targetId) {
        String key = tuple(sourceId, targetId);
        CountedEdge current = edges.get(key);
        if (current == null) {
            edges.put(key, new CountedEdge(sourceId, targetId));
        } else {
            current.count++;
        }
    }

    private static List<String> eventValues(NormalizedMemoryEvent event) {
        String kind = EMPTY;
        String value = EMPTY;
        if (event.getFactValue().isPresent()) {
            kind = event.getFactValue().get().getKind().name();
            value = event.getFactValue().get().getValue();
        }
        return fields(
            event.getOperation().name(),
            event.getFactId(),
            event.getFactKey().getSubjectId(),
            event.getFactKey().getPredicate(),
            event.getFactKey().getScope(),
            kind,
            value,
            instant(event.getValidTime().getStart()),
            optionalInstant(event.getValidTime()),
            instant(event.getRecordedAt()),
            event.getPayloadHash());
    }

    private static List<String> versionValues(
        MemoryFactVersion version,
        FactKey key) {
        MemoryFact fact = version.getFact();
        String kind = fact.isRelationship()
            ? FactValue.Kind.ENTITY_REF.name()
            : FactValue.Kind.LITERAL.name();
        String literalValue = fact.getLiteralValue().orElse(EMPTY);
        return fields(
            fact.getId(),
            fact.getPredicate(),
            key.getScope(),
            kind,
            literalValue,
            version.getStatus().name(),
            instant(version.getValidTime().getStart()),
            optionalInstant(version.getValidTime()),
            instant(version.getTransactionTime().getStart()),
            optionalInstant(version.getTransactionTime()));
    }

    private static void collectEventEntities(
        NormalizedMemoryEvent event,
        Map<String, MemoryEntity> entities) {
        if (!event.getEvent().getFact().isPresent()) {
            return;
        }
        MemoryFact fact = event.getEvent().getFact().get();
        collectEntity(entities, fact.getSubject());
        if (fact.isRelationship()) {
            collectEntity(entities, fact.getTarget().get());
        }
    }

    private static void collectEntity(
        Map<String, MemoryEntity> entities,
        MemoryEntity entity) {
        putUnique(entities, entity.getId(), entity, "entity");
    }

    private static void collectEvidence(
        List<Evidence> evidence,
        Map<String, Evidence> evidenceById,
        Map<String, Source> sources) {
        for (Evidence item : evidence) {
            putUnique(evidenceById, item.getId(), item, "evidence");
            putUnique(
                sources,
                item.getSource().getId(),
                item.getSource(),
                "source");
        }
    }

    private static <T> void putUnique(
        Map<String, T> values,
        String id,
        T value,
        String type) {
        T previous = values.get(id);
        if (previous != null && !previous.equals(value)) {
            throw new IllegalArgumentException(
                "Conflicting " + type + " id: " + id);
        }
        values.put(id, value);
    }

    private static void putUniqueVertex(
        Map<String, Vertex> vertices,
        Vertex vertex) {
        Vertex previous = vertices.put(vertex.getId(), vertex);
        if (previous != null
            && !previous.getValues().equals(vertex.getValues())) {
            throw new IllegalArgumentException(
                "Conflicting vertex id: " + vertex.getId());
        }
    }

    private static void validateGraphShape(MemoryGraph graph) {
        GraphSchema schema = Objects.requireNonNull(
            graph.getGraphSchema(), "graphSchema");
        require(
            schema.getVertexSchemaList().size() == VERTEX_LABELS.size(),
            "Unexpected vertex schema count");
        require(
            schema.getEdgeSchemaList().size() == EDGE_LABELS.size(),
            "Unexpected edge schema count");

        for (int index = 0; index < VERTEX_LABELS.size(); index++) {
            String label = VERTEX_LABELS.get(index);
            VertexSchema actual = schema.getVertexSchemaList().get(index);
            require(label.equals(actual.getLabel()),
                "Unexpected vertex schema: " + actual.getLabel());
            require("id".equals(actual.getIdField()),
                "Unexpected vertex id field: " + label);
            require(vertexFields(label).equals(actual.getFields()),
                "Unexpected vertex fields: " + label);
        }
        for (int index = 0; index < EDGE_LABELS.size(); index++) {
            String label = EDGE_LABELS.get(index);
            EdgeSchema actual = schema.getEdgeSchemaList().get(index);
            require(label.equals(actual.getLabel()),
                "Unexpected edge schema: " + actual.getLabel());
            require("srcId".equals(actual.getSrcIdField()),
                "Unexpected edge source field: " + label);
            require("dstId".equals(actual.getDstIdField()),
                "Unexpected edge target field: " + label);
            require(edgeFields(label).equals(actual.getFields()),
                "Unexpected edge fields: " + label);
        }

        Map<String, EntityGroup> groups = Objects.requireNonNull(
            graph.entities, "graph.entities");
        List<String> expectedGroups = new ArrayList<>(VERTEX_LABELS);
        expectedGroups.addAll(EDGE_LABELS);
        require(
            expectedGroups.equals(new ArrayList<>(groups.keySet())),
            "Unexpected graph entity groups");
        for (String label : VERTEX_LABELS) {
            require(groups.get(label) instanceof VertexGroup,
                "Expected vertex group: " + label);
        }
        for (String label : EDGE_LABELS) {
            require(groups.get(label) instanceof EdgeGroup,
                "Expected edge group: " + label);
        }
    }

    private static Map<String, Vertex> vertexIndex(
        MemoryGraph graph,
        String label,
        String prefix,
        List<String> expectedFields) {
        Map<String, Vertex> result = new TreeMap<>();
        for (Vertex vertex
            : ((VertexGroup) graph.entities.get(label)).getVertices()) {
            require(vertex != null, "Null vertex in group: " + label);
            require(label.equals(vertex.getLabel()),
                "Vertex label does not match group: " + vertex.getId());
            rawId(vertex.getId(), prefix);
            require(vertex.getValues() != null,
                "Null vertex values: " + vertex.getId());
            require(vertex.getValues().size() == expectedFields.size(),
                "Unexpected vertex value count: " + vertex.getId());
            for (String fieldValue : vertex.getValues()) {
                require(fieldValue != null,
                    "Null vertex field: " + vertex.getId());
            }
            require(result.put(vertex.getId(), vertex) == null,
                "Duplicate vertex id: " + vertex.getId());
        }
        return result;
    }

    private static Map<String, List<Edge>> edgeIndex(MemoryGraph graph) {
        Map<String, List<Edge>> result = new LinkedHashMap<>();
        for (String label : EDGE_LABELS) {
            List<Edge> rows = new ArrayList<>();
            Set<Edge> identities = new HashSet<>();
            for (Edge edge
                : ((EdgeGroup) graph.entities.get(label)).getOutEdges()) {
                require(edge != null, "Null edge in group: " + label);
                require(label.equals(edge.getLabel()),
                    "Edge label does not match group: " + label);
                require(edge.getSrcId() != null && edge.getDstId() != null,
                    "Null edge endpoint: " + label);
                require(edge.getValues() != null,
                    "Null edge values: " + label);
                require(edge.getValues().size() == edgeFields(label).size(),
                    "Unexpected edge value count: " + label);
                for (String fieldValue : edge.getValues()) {
                    require(fieldValue != null,
                        "Null edge field: " + label);
                }
                require(identities.add(edge),
                    "Duplicate edge identity: " + edge);
                rows.add(edge);
            }
            Collections.sort(rows, EDGE_ORDER);
            result.put(label, rows);
        }
        return result;
    }

    private static Map<String, MemoryEntity> readEntities(
        Map<String, Vertex> vertices) {
        Map<String, MemoryEntity> result = new TreeMap<>();
        for (Vertex vertex : vertices.values()) {
            String rawId = rawId(vertex.getId(), ENTITY_PREFIX);
            result.put(
                vertex.getId(),
                new MemoryEntity(
                    rawId,
                    value(vertex, ENTITY_FIELDS, "label")));
        }
        return result;
    }

    private static Map<String, Source> readSources(
        Map<String, Vertex> vertices) {
        Map<String, Source> result = new TreeMap<>();
        for (Vertex vertex : vertices.values()) {
            String rawId = rawId(vertex.getId(), SOURCE_PREFIX);
            result.put(
                vertex.getId(),
                new Source(
                    rawId,
                    value(vertex, SOURCE_FIELDS, "name")));
        }
        return result;
    }

    private static Map<String, Evidence> readEvidence(
        Map<String, Vertex> vertices,
        Map<String, Source> sources,
        List<Edge> fromSourceEdges) {
        Map<String, String> sourceByEvidence = new HashMap<>();
        for (Edge edge : fromSourceEdges) {
            require(vertices.containsKey(edge.getSrcId()),
                "Unknown evidence in from_source edge");
            require(sources.containsKey(edge.getDstId()),
                "Unknown source in from_source edge");
            require(sourceByEvidence.put(
                edge.getSrcId(), edge.getDstId()) == null,
                "Evidence has multiple sources: " + edge.getSrcId());
        }
        require(sourceByEvidence.keySet().equals(vertices.keySet()),
            "Every evidence must have exactly one source");

        Map<String, Evidence> result = new TreeMap<>();
        for (Vertex vertex : vertices.values()) {
            String rawId = rawId(vertex.getId(), EVIDENCE_PREFIX);
            result.put(
                vertex.getId(),
                new Evidence(
                    rawId,
                    sources.get(sourceByEvidence.get(vertex.getId())),
                    value(vertex, EVIDENCE_FIELDS, "content")));
        }
        return result;
    }

    private static Map<String, List<Evidence>> readSupportedEvidence(
        List<Edge> supportedEdges,
        Map<String, Vertex> eventVertices,
        Map<String, Vertex> versionVertices,
        Map<String, Evidence> evidence) {
        Map<String, List<Evidence>> result = new HashMap<>();
        for (Edge edge : supportedEdges) {
            require(
                eventVertices.containsKey(edge.getSrcId())
                    || versionVertices.containsKey(edge.getSrcId()),
                "Unknown supported_by owner: " + edge.getSrcId());
            Evidence item = evidence.get(edge.getDstId());
            require(item != null,
                "Unknown supported_by evidence: " + edge.getDstId());
            int count = positiveCount(edge.getValues().get(0));
            List<Evidence> ownerEvidence = result.computeIfAbsent(
                edge.getSrcId(), ignored -> new ArrayList<>());
            for (int occurrence = 0; occurrence < count; occurrence++) {
                ownerEvidence.add(item);
            }
        }
        return result;
    }

    private static Map<String, NormalizedMemoryEvent> readEvents(
        Map<String, Vertex> vertices,
        Map<String, MemoryEntity> entities,
        Map<String, List<Evidence>> evidenceByOwner) {
        Map<String, NormalizedMemoryEvent> result = new TreeMap<>();
        EventNormalizer normalizer = new EventNormalizer();
        for (Vertex vertex : vertices.values()) {
            String rawEventId = rawId(vertex.getId(), EVENT_PREFIX);
            MemoryEventOperation operation = enumValue(
                MemoryEventOperation.class,
                value(vertex, EVENT_FIELDS, "operation"),
                "event operation");
            String factId = value(vertex, EVENT_FIELDS, "factId");
            String subjectId = value(vertex, EVENT_FIELDS, "subjectId");
            String predicate = value(vertex, EVENT_FIELDS, "predicate");
            FactKey key = new FactKey(
                subjectId,
                predicate,
                value(vertex, EVENT_FIELDS, "scope"));
            TimeInterval validTime = interval(
                value(vertex, EVENT_FIELDS, "validStart"),
                value(vertex, EVENT_FIELDS, "validEnd"));
            Instant recordedAt = parseInstant(
                value(vertex, EVENT_FIELDS, "recordedAt"));
            List<Evidence> eventEvidence = evidenceByOwner.get(
                vertex.getId());
            require(eventEvidence != null && !eventEvidence.isEmpty(),
                "Memory event must have evidence: " + rawEventId);

            MemoryEvent event;
            if (operation == MemoryEventOperation.RETRACT) {
                require(value(vertex, EVENT_FIELDS, "valueKind").isEmpty(),
                    "Retract event must not have a value kind");
                require(value(vertex, EVENT_FIELDS, "value").isEmpty(),
                    "Retract event must not have a value");
                event = MemoryEvent.retract(
                    rawEventId,
                    factId,
                    validTime,
                    recordedAt,
                    eventEvidence);
            } else {
                MemoryFact fact = eventFact(
                    vertex,
                    factId,
                    subjectId,
                    predicate,
                    entities);
                if (operation == MemoryEventOperation.ADD) {
                    event = MemoryEvent.add(
                        rawEventId,
                        fact,
                        validTime,
                        recordedAt,
                        eventEvidence);
                } else {
                    require(operation == MemoryEventOperation.CORRECT,
                        "Unsupported memory event operation");
                    event = MemoryEvent.correct(
                        rawEventId,
                        fact,
                        validTime,
                        recordedAt,
                        eventEvidence);
                }
            }

            NormalizedMemoryEvent normalized = normalizer.normalize(event, key);
            require(eventId(normalized.getEventId()).equals(vertex.getId()),
                "Event id is not canonical: " + rawEventId);
            require(
                normalized.getPayloadHash().equals(
                    value(vertex, EVENT_FIELDS, "payloadHash")),
                "Event payload hash does not match: " + rawEventId);
            result.put(vertex.getId(), normalized);
        }
        return result;
    }

    private static MemoryFact eventFact(
        Vertex vertex,
        String factId,
        String subjectId,
        String predicate,
        Map<String, MemoryEntity> entities) {
        MemoryEntity subject = entities.get(entityId(subjectId));
        require(subject != null,
            "Unknown event subject: " + subjectId);
        FactValue.Kind kind = enumValue(
            FactValue.Kind.class,
            value(vertex, EVENT_FIELDS, "valueKind"),
            "event value kind");
        String factValue = value(vertex, EVENT_FIELDS, "value");
        if (kind == FactValue.Kind.LITERAL) {
            return MemoryFact.attribute(
                factId, subject, predicate, factValue);
        }
        MemoryEntity target = entities.get(entityId(factValue));
        require(target != null,
            "Unknown event object: " + factValue);
        return MemoryFact.relationship(
            factId, subject, predicate, target);
    }

    private static Map<FactKey, List<MemoryFactVersion>> readVersions(
        Map<String, Vertex> vertices,
        Map<String, MemoryEntity> entities,
        Map<String, List<Evidence>> evidenceByOwner,
        List<Edge> subjectEdges,
        List<Edge> objectEdges) {
        Map<String, String> subjects = uniqueTargets(
            subjectEdges,
            vertices,
            entities,
            "subject");
        require(subjects.keySet().equals(vertices.keySet()),
            "Every fact version must have exactly one subject");
        Map<String, String> objects = uniqueTargets(
            objectEdges,
            vertices,
            entities,
            "object");

        Map<FactKey, List<MemoryFactVersion>> result = new TreeMap<>();
        for (Vertex vertex : vertices.values()) {
            String rawVersionId = rawId(vertex.getId(), VERSION_PREFIX);
            MemoryEntity subject = entities.get(subjects.get(vertex.getId()));
            String predicate = value(vertex, VERSION_FIELDS, "predicate");
            FactKey key = new FactKey(
                subject.getId(),
                predicate,
                value(vertex, VERSION_FIELDS, "scope"));

            FactValue.Kind kind = enumValue(
                FactValue.Kind.class,
                value(vertex, VERSION_FIELDS, "valueKind"),
                "version value kind");
            String factId = value(vertex, VERSION_FIELDS, "factId");
            String literalValue = value(
                vertex, VERSION_FIELDS, "literalValue");
            MemoryFact fact;
            if (kind == FactValue.Kind.LITERAL) {
                require(!objects.containsKey(vertex.getId()),
                    "Literal version must not have an object");
                fact = MemoryFact.attribute(
                    factId, subject, predicate, literalValue);
            } else {
                require(literalValue.isEmpty(),
                    "Entity-reference version must not have a literal value");
                String objectId = objects.get(vertex.getId());
                require(objectId != null,
                    "Entity-reference version must have an object");
                fact = MemoryFact.relationship(
                    factId, subject, predicate, entities.get(objectId));
            }

            List<Evidence> versionEvidence = evidenceByOwner.get(
                vertex.getId());
            require(versionEvidence != null && !versionEvidence.isEmpty(),
                "Fact version must have evidence: " + rawVersionId);
            MemoryFactVersion version = new MemoryFactVersion(
                rawVersionId,
                fact,
                enumValue(
                    MemoryFactVersionStatus.class,
                    value(vertex, VERSION_FIELDS, "status"),
                    "version status"),
                interval(
                    value(vertex, VERSION_FIELDS, "validStart"),
                    value(vertex, VERSION_FIELDS, "validEnd")),
                interval(
                    value(vertex, VERSION_FIELDS, "transactionStart"),
                    value(vertex, VERSION_FIELDS, "transactionEnd")),
                versionEvidence);
            result.computeIfAbsent(
                key, ignored -> new ArrayList<>()).add(version);
        }
        return result;
    }

    private static Map<String, String> uniqueTargets(
        List<Edge> edges,
        Map<String, Vertex> sourceVertices,
        Map<String, ?> targets,
        String relationName) {
        Map<String, String> result = new HashMap<>();
        for (Edge edge : edges) {
            require(sourceVertices.containsKey(edge.getSrcId()),
                "Unknown " + relationName + " source");
            require(targets.containsKey(edge.getDstId()),
                "Unknown " + relationName + " target");
            require(result.put(edge.getSrcId(), edge.getDstId()) == null,
                "Multiple " + relationName + " targets");
        }
        return result;
    }

    private static void validateNoOrphanVertices(
        Map<String, MemoryEntity> entities,
        Map<String, Evidence> evidence,
        Map<String, Source> sources,
        Map<String, NormalizedMemoryEvent> events,
        Map<FactKey, List<MemoryFactVersion>> versions) {
        Map<String, MemoryEntity> referencedEntities = new TreeMap<>();
        Map<String, Evidence> referencedEvidence = new TreeMap<>();
        Map<String, Source> referencedSources = new TreeMap<>();
        for (NormalizedMemoryEvent event : events.values()) {
            collectEventEntities(event, referencedEntities);
            collectEvidence(
                event.getEvidence(),
                referencedEvidence,
                referencedSources);
        }
        for (List<MemoryFactVersion> factVersions : versions.values()) {
            for (MemoryFactVersion version : factVersions) {
                MemoryFact fact = version.getFact();
                collectEntity(referencedEntities, fact.getSubject());
                if (fact.isRelationship()) {
                    collectEntity(
                        referencedEntities,
                        fact.getTarget().get());
                }
                collectEvidence(
                    version.getEvidence(),
                    referencedEvidence,
                    referencedSources);
            }
        }
        requireAllReferenced(
            entities, referencedEntities, ENTITY_PREFIX, "entity");
        requireAllReferenced(
            evidence, referencedEvidence, EVIDENCE_PREFIX, "evidence");
        requireAllReferenced(
            sources, referencedSources, SOURCE_PREFIX, "source");
    }

    private static void requireAllReferenced(
        Map<String, ?> graphValues,
        Map<String, ?> referencedValues,
        String prefix,
        String type) {
        for (String graphId : graphValues.keySet()) {
            require(referencedValues.containsKey(rawId(graphId, prefix)),
                "Orphan " + type + " vertex: " + graphId);
        }
    }

    private static Map<String, String> readGeneratingEvents(
        List<Edge> generateEdges,
        Map<String, NormalizedMemoryEvent> events,
        Map<String, Vertex> versions) {
        Map<String, String> byVersion = new HashMap<>();
        for (Edge edge : generateEdges) {
            require(events.containsKey(edge.getSrcId()),
                "Unknown generating event: " + edge.getSrcId());
            require(versions.containsKey(edge.getDstId()),
                "Unknown generated version: " + edge.getDstId());
            require(byVersion.put(
                edge.getDstId(), edge.getSrcId()) == null,
                "Version has multiple generating events: "
                    + edge.getDstId());
        }
        require(byVersion.keySet().equals(versions.keySet()),
            "Every fact version must have one generating event");

        Map<String, String> result = new TreeMap<>();
        for (Map.Entry<String, String> entry : byVersion.entrySet()) {
            result.put(
                rawId(entry.getKey(), VERSION_PREFIX),
                rawId(entry.getValue(), EVENT_PREFIX));
        }
        return result;
    }

    private static List<VersionRelation> readRelations(
        Map<String, List<Edge>> edges,
        Map<String, Vertex> versions) {
        List<VersionRelation> result = new ArrayList<>();
        for (String label : Arrays.asList(
            SUPERSEDES, DUPLICATE_OF, CONFLICTS_WITH)) {
            for (Edge edge : edges.get(label)) {
                require(versions.containsKey(edge.getSrcId()),
                    "Unknown relation source: " + edge.getSrcId());
                require(versions.containsKey(edge.getDstId()),
                    "Unknown relation target: " + edge.getDstId());
                require(
                    relationId(label, edge.getSrcId(), edge.getDstId())
                        .equals(edge.getValues().get(0)),
                    "Version relation id does not match endpoints");
                int count = positiveCount(edge.getValues().get(1));
                for (int occurrence = 0; occurrence < count; occurrence++) {
                    VersionRelation relation = new VersionRelation(
                        relationType(label),
                        rawId(edge.getSrcId(), VERSION_PREFIX),
                        rawId(edge.getDstId(), VERSION_PREFIX));
                    require(
                        versionId(relation.getFromVersionId()).equals(
                            edge.getSrcId())
                            && versionId(relation.getToVersionId()).equals(
                                edge.getDstId()),
                        "Version relation endpoints are not canonical");
                    result.add(relation);
                }
            }
        }
        return result;
    }

    private static List<String> vertexFields(String label) {
        if (ENTITY.equals(label)) {
            return ENTITY_FIELDS;
        }
        if (FACT_VERSION.equals(label)) {
            return VERSION_FIELDS;
        }
        if (MEMORY_EVENT.equals(label)) {
            return EVENT_FIELDS;
        }
        if (EVIDENCE.equals(label)) {
            return EVIDENCE_FIELDS;
        }
        if (SOURCE.equals(label)) {
            return SOURCE_FIELDS;
        }
        throw new IllegalArgumentException(
            "Unknown vertex label: " + label);
    }

    private static List<String> edgeFields(String label) {
        if (SUPPORTED_BY.equals(label)) {
            return COUNT_FIELDS;
        }
        if (SUPERSEDES.equals(label)
            || DUPLICATE_OF.equals(label)
            || CONFLICTS_WITH.equals(label)) {
            return RELATION_FIELDS;
        }
        if (SUBJECT.equals(label)
            || OBJECT.equals(label)
            || GENERATES.equals(label)
            || FROM_SOURCE.equals(label)) {
            return NO_FIELDS;
        }
        throw new IllegalArgumentException(
            "Unknown edge label: " + label);
    }

    private static String value(
        Vertex vertex,
        List<String> fields,
        String field) {
        int index = fields.indexOf(field);
        if (index < 0) {
            throw new IllegalArgumentException("Unknown field: " + field);
        }
        return vertex.getValues().get(index);
    }

    private static TimeInterval interval(String start, String end) {
        Instant parsedStart = parseInstant(start);
        return end.isEmpty()
            ? TimeInterval.unboundedFrom(parsedStart)
            : new TimeInterval(parsedStart, parseInstant(end));
    }

    private static Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException(
                "Invalid instant: " + value,
                exception);
        }
    }

    private static int positiveCount(String value) {
        try {
            int count = Integer.parseInt(value);
            require(count > 0, "Occurrence count must be positive");
            return count;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                "Invalid occurrence count: " + value,
                exception);
        }
    }

    private static <T extends Enum<T>> T enumValue(
        Class<T> type,
        String value,
        String fieldName) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                "Invalid " + fieldName + ": " + value,
                exception);
        }
    }

    private static String relationLabel(VersionRelationType type) {
        if (type == VersionRelationType.SUPERSEDES) {
            return SUPERSEDES;
        }
        if (type == VersionRelationType.DUPLICATE_OF) {
            return DUPLICATE_OF;
        }
        if (type == VersionRelationType.CONFLICTS_WITH) {
            return CONFLICTS_WITH;
        }
        throw new IllegalArgumentException(
            "Unsupported version relation type: " + type);
    }

    private static VersionRelationType relationType(String label) {
        if (SUPERSEDES.equals(label)) {
            return VersionRelationType.SUPERSEDES;
        }
        if (DUPLICATE_OF.equals(label)) {
            return VersionRelationType.DUPLICATE_OF;
        }
        if (CONFLICTS_WITH.equals(label)) {
            return VersionRelationType.CONFLICTS_WITH;
        }
        throw new IllegalArgumentException(
            "Unsupported version relation label: " + label);
    }

    private static String relationId(
        String label,
        String sourceId,
        String targetId) {
        return "relation:" + tuple(label, sourceId, targetId);
    }

    private static String tuple(String... values) {
        StringBuilder result = new StringBuilder();
        for (String value : values) {
            result.append(value.length()).append(':').append(value);
        }
        return result.toString();
    }

    private static String instant(Instant instant) {
        return instant.toString();
    }

    private static String optionalInstant(TimeInterval interval) {
        return interval.getEnd().isPresent()
            ? instant(interval.getEnd().get()) : EMPTY;
    }

    private static String entityId(String rawId) {
        return ENTITY_PREFIX + rawId;
    }

    private static String versionId(String rawId) {
        return VERSION_PREFIX + rawId;
    }

    private static String eventId(String rawId) {
        return EVENT_PREFIX + rawId;
    }

    private static String evidenceId(String rawId) {
        return EVIDENCE_PREFIX + rawId;
    }

    private static String sourceId(String rawId) {
        return SOURCE_PREFIX + rawId;
    }

    private static String rawId(String graphId, String prefix) {
        require(graphId != null && graphId.startsWith(prefix),
            "Graph id has the wrong prefix: " + graphId);
        String rawId = graphId.substring(prefix.length());
        require(!rawId.trim().isEmpty(),
            "Graph id has an empty raw id: " + graphId);
        return rawId;
    }

    private static List<String> fields(String... values) {
        return Collections.unmodifiableList(Arrays.asList(values));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static final class CountedEdge {

        private final String sourceId;
        private final String targetId;
        private int count;

        private CountedEdge(String sourceId, String targetId) {
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.count = 1;
        }
    }
}
