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

package org.apache.geaflow.ai.casts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.Schema;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.vector.CastsVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.operator.SearchOperator;
import org.apache.geaflow.ai.protocol.AiScope;
import org.apache.geaflow.ai.protocol.AiTrace;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastsOperator implements SearchOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CastsOperator.class);

    private final GraphAccessor graphAccessor;
    private final CastsRestClient castsClient;

    public CastsOperator(GraphAccessor graphAccessor, CastsRestClient castsClient) {
        this.graphAccessor = Objects.requireNonNull(graphAccessor);
        this.castsClient = Objects.requireNonNull(castsClient);
    }

    @Override
    public List<SubGraph> apply(List<SubGraph> subGraphList, VectorSearch search) {
        List<IVector> castsVectors = search.getVectorMap().get(VectorType.CastsVector);
        if (castsVectors == null || castsVectors.isEmpty()) {
            return subGraphList == null ? new ArrayList<>() : new ArrayList<>(subGraphList);
        }

        CastsVector castsVector = (CastsVector) castsVectors.get(0);
        String goal = castsVector.getGoal();
        int maxDepth = castsVector.getMaxDepth() > 0 ? castsVector.getMaxDepth() : 5;

        String sessionId = search.getSessionId();
        String runId = sessionId;

        if (subGraphList == null || subGraphList.isEmpty()) {
            return new ArrayList<>();
        }

        String schemaFingerprint = buildSchemaFingerprint(graphAccessor.getGraphSchema());
        String schemaSummary = buildSchemaSummary(graphAccessor.getGraphSchema());

        List<SubGraph> out = new ArrayList<>(subGraphList.size());
        for (SubGraph subGraph : subGraphList) {
            out.add(applyOneSubgraph(subGraph, goal, maxDepth, runId, schemaFingerprint, schemaSummary));
        }
        return out;
    }

    private SubGraph applyOneSubgraph(
        SubGraph subGraph,
        String goal,
        int maxDepth,
        String runId,
        String schemaFingerprint,
        String schemaSummary
    ) {
        Set<GraphEntity> entitySet = new HashSet<>(subGraph.getGraphEntityList());

        GraphVertex currentVertex = pickCurrentVertex(subGraph);
        if (currentVertex == null) {
            return subGraph;
        }

        GraphEntity current = currentVertex;
        String entryVertexIdForEdge = currentVertex.getVertex().getId();

        String signature = "V()";
        AiTrace trace = AiTrace.newTrace("geaflow-ai");
        AiScope scope = AiScope.withRunId(runId);

        for (int step = 0; step < maxDepth; step++) {
            CastsDecisionRequest request = buildRequest(
                goal,
                signature,
                step,
                current,
                schemaFingerprint,
                schemaSummary
            );

            CastsDecisionResponse resp;
            try {
                resp = castsClient.decision(scope, trace, request);
            } catch (Exception e) {
                LOGGER.warn("CASTS service call failed, stop traversal: {}", e.getMessage());
                break;
            }

            if (resp == null || resp.decision == null) {
                break;
            }

            CastsDecisionParser.ParsedDecision parsed = CastsDecisionParser.parse(resp.decision);
            if (parsed.kind == CastsDecisionParser.Kind.STOP || parsed.kind == CastsDecisionParser.Kind.UNKNOWN) {
                break;
            }

            boolean progressed;
            TraversalResult tr = executeDecision(current, entryVertexIdForEdge, parsed);
            progressed = tr.progressed;
            if (!progressed) {
                break;
            }

            // Apply entities into the subgraph (dedupe).
            for (GraphEntity e : tr.addEntities) {
                if (entitySet.add(e)) {
                    subGraph.addEntity(e);
                }
            }

            // Move cursor.
            current = tr.nextEntity;
            if (current instanceof GraphVertex) {
                entryVertexIdForEdge = ((GraphVertex) current).getVertex().getId();
            } else if (current instanceof GraphEdge) {
                entryVertexIdForEdge = tr.entryVertexIdForEdge != null ? tr.entryVertexIdForEdge : entryVertexIdForEdge;
            }

            signature = signature + "." + parsed.raw;
        }

        return subGraph;
    }

    private static class TraversalResult {
        final boolean progressed;
        final GraphEntity nextEntity;
        final List<GraphEntity> addEntities;
        final String entryVertexIdForEdge;

        TraversalResult(boolean progressed, GraphEntity nextEntity, List<GraphEntity> addEntities, String entryVertexIdForEdge) {
            this.progressed = progressed;
            this.nextEntity = nextEntity;
            this.addEntities = addEntities;
            this.entryVertexIdForEdge = entryVertexIdForEdge;
        }
    }

    private TraversalResult executeDecision(GraphEntity current, String entryVertexIdForEdge, CastsDecisionParser.ParsedDecision parsed) {
        if (current instanceof GraphVertex) {
            GraphVertex v = (GraphVertex) current;
            String vid = v.getVertex().getId();
            List<GraphEdge> edges = expandEdges(v);

            if (parsed.kind == CastsDecisionParser.Kind.OUT) {
                List<GraphEdge> outEdges = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getSrcId()))
                    .collect(Collectors.toList());
                return traverseToVertexFromEdges(vid, outEdges, true);
            }

            if (parsed.kind == CastsDecisionParser.Kind.IN) {
                List<GraphEdge> inEdges = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getDstId()))
                    .collect(Collectors.toList());
                return traverseToVertexFromEdges(vid, inEdges, false);
            }

            if (parsed.kind == CastsDecisionParser.Kind.BOTH) {
                List<GraphEdge> bothEdges = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getSrcId()) || vid.equals(e.getEdge().getDstId()))
                    .collect(Collectors.toList());
                if (bothEdges.isEmpty()) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                List<GraphEntity> add = new ArrayList<>();
                add.addAll(bothEdges);
                GraphVertex next = null;
                for (GraphEdge e : bothEdges) {
                    boolean isOut = vid.equals(e.getEdge().getSrcId());
                    String nextId = isOut ? e.getEdge().getDstId() : e.getEdge().getSrcId();
                    GraphVertex v2 = graphAccessor.getVertex(null, nextId);
                    if (v2 != null) {
                        add.add(v2);
                        if (next == null) {
                            next = v2;
                        }
                    }
                }
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, add, vid);
            }

            if (parsed.kind == CastsDecisionParser.Kind.OUT_E) {
                GraphEdge next = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getSrcId()))
                    .findFirst()
                    .orElse(null);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), vid);
            }

            if (parsed.kind == CastsDecisionParser.Kind.IN_E) {
                GraphEdge next = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getDstId()))
                    .findFirst()
                    .orElse(null);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), vid);
            }

            if (parsed.kind == CastsDecisionParser.Kind.BOTH_E) {
                GraphEdge next = edges.stream()
                    .filter(e -> parsed.label.equals(e.getLabel()))
                    .filter(e -> vid.equals(e.getEdge().getSrcId()) || vid.equals(e.getEdge().getDstId()))
                    .findFirst()
                    .orElse(null);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), vid);
            }

            // Filter/modifier steps are treated as no-op for execution.
            return new TraversalResult(true, current, List.of(), null);
        }

        if (current instanceof GraphEdge) {
            GraphEdge e = (GraphEdge) current;
            String srcId = e.getEdge().getSrcId();
            String dstId = e.getEdge().getDstId();

            if (parsed.kind == CastsDecisionParser.Kind.IN_V) {
                GraphVertex next = graphAccessor.getVertex(null, dstId);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), null);
            }
            if (parsed.kind == CastsDecisionParser.Kind.OUT_V) {
                GraphVertex next = graphAccessor.getVertex(null, srcId);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), null);
            }
            if (parsed.kind == CastsDecisionParser.Kind.OTHER_V) {
                String nextId = entryVertexIdForEdge != null && entryVertexIdForEdge.equals(srcId) ? dstId : srcId;
                GraphVertex next = graphAccessor.getVertex(null, nextId);
                if (next == null) {
                    return new TraversalResult(false, current, List.of(), null);
                }
                return new TraversalResult(true, next, new ArrayList<>(List.of(next)), null);
            }

            // Modifier steps as no-op.
            return new TraversalResult(true, current, List.of(), null);
        }

        return new TraversalResult(false, current, List.of(), null);
    }

    private TraversalResult traverseToVertexFromEdges(String currentVertexId, List<GraphEdge> edges, boolean outgoing) {
        if (edges == null || edges.isEmpty()) {
            return new TraversalResult(false, null, List.of(), null);
        }
        List<GraphEntity> add = new ArrayList<>();
        add.addAll(edges);

        GraphVertex next = null;
        for (GraphEdge e : edges) {
            String nextId = outgoing ? e.getEdge().getDstId() : e.getEdge().getSrcId();
            GraphVertex v = graphAccessor.getVertex(null, nextId);
            if (v != null) {
                add.add(v);
                if (next == null) {
                    next = v;
                }
            }
        }
        if (next == null) {
            return new TraversalResult(false, null, List.of(), null);
        }
        return new TraversalResult(true, next, add, currentVertexId);
    }

    private List<GraphEdge> expandEdges(GraphVertex v) {
        List<GraphEntity> expanded = graphAccessor.expand(v);
        List<GraphEdge> edges = new ArrayList<>();
        for (GraphEntity e : expanded) {
            if (e instanceof GraphEdge) {
                edges.add((GraphEdge) e);
            }
        }
        return edges;
    }

    private GraphVertex pickCurrentVertex(SubGraph subGraph) {
        List<GraphEntity> entities = subGraph.getGraphEntityList();
        for (int i = entities.size() - 1; i >= 0; i--) {
            GraphEntity e = entities.get(i);
            if (e instanceof GraphVertex) {
                return (GraphVertex) e;
            }
        }
        return null;
    }

    private CastsDecisionRequest buildRequest(
        String goal,
        String signature,
        int stepIndex,
        GraphEntity current,
        String schemaFingerprint,
        String schemaSummary
    ) {
        CastsDecisionRequest req = new CastsDecisionRequest();
        req.goal = goal;
        req.traversal = new CastsDecisionRequest.Traversal();
        req.traversal.structuralSignature = signature;
        req.traversal.stepIndex = stepIndex;

        req.node = new CastsDecisionRequest.Node();
        req.node.properties = extractProperties(current);
        req.node.label = current.getLabel();

        if (!req.node.properties.containsKey("type")) {
            req.node.properties.put("type", current.getLabel());
        }

        List<String> outgoing = new ArrayList<>();
        List<String> incoming = new ArrayList<>();
        if (current instanceof GraphVertex) {
            GraphVertex v = (GraphVertex) current;
            String vid = v.getVertex().getId();
            for (GraphEdge e : expandEdges(v)) {
                if (vid.equals(e.getEdge().getSrcId())) {
                    outgoing.add(e.getLabel());
                } else if (vid.equals(e.getEdge().getDstId())) {
                    incoming.add(e.getLabel());
                }
            }
        }

        req.graphSchema = new CastsDecisionRequest.GraphSchema();
        req.graphSchema.schemaFingerprint = schemaFingerprint;
        req.graphSchema.schemaSummary = schemaSummary;
        req.graphSchema.validOutgoingLabels = outgoing.stream().distinct().sorted().collect(Collectors.toList());
        req.graphSchema.validIncomingLabels = incoming.stream().distinct().sorted().collect(Collectors.toList());

        return req;
    }

    private Map<String, Object> extractProperties(GraphEntity entity) {
        Map<String, Object> props = new HashMap<>();
        if (entity instanceof GraphVertex) {
            GraphVertex v = (GraphVertex) entity;
            Schema schema = graphAccessor.getGraphSchema().getSchema(v.getLabel());
            if (schema instanceof VertexSchema) {
                List<String> fields = ((VertexSchema) schema).getFields();
                List<String> values = v.getVertex().getValues();
                for (int i = 0; i < Math.min(fields.size(), values.size()); i++) {
                    props.put(fields.get(i), values.get(i));
                }
            }
            props.put("id", v.getVertex().getId());
        } else if (entity instanceof GraphEdge) {
            GraphEdge e = (GraphEdge) entity;
            Schema schema = graphAccessor.getGraphSchema().getSchema(e.getLabel());
            if (schema instanceof EdgeSchema) {
                List<String> fields = ((EdgeSchema) schema).getFields();
                List<String> values = e.getEdge().getValues();
                for (int i = 0; i < Math.min(fields.size(), values.size()); i++) {
                    props.put(fields.get(i), values.get(i));
                }
            }
            props.put("srcId", e.getEdge().getSrcId());
            props.put("dstId", e.getEdge().getDstId());
        }
        return props;
    }

    private static String buildSchemaFingerprint(GraphSchema schema) {
        if (schema == null) {
            return "schema_unknown";
        }
        String name = schema.getName();
        return "schema_" + (name == null ? "unknown" : name);
    }

    private static String buildSchemaSummary(GraphSchema schema) {
        if (schema == null) {
            return "";
        }
        List<String> nodes = schema.getVertexSchemaList().stream()
            .map(VertexSchema::getLabel)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        List<String> edges = schema.getEdgeSchemaList().stream()
            .map(EdgeSchema::getLabel)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        return "node_types=" + nodes + ", edge_labels=" + edges;
    }
}
