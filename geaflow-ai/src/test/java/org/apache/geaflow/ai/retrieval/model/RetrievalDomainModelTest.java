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

import com.google.gson.JsonParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Regression coverage for normalized retrieval graph models and evidence JSON. */
public class RetrievalDomainModelTest {

    @Test
    public void graphPathRetainsCompleteOrderedReferencesAndDerivesIds() {
        GraphVertexRef first = vertex("v1", "e1");
        GraphVertexRef second = vertex("v2", "e2");
        GraphEdgeRef edge = edge("edge-1", "e1", "e2");
        GraphPathRef path = new GraphPathRef(Arrays.asList(first, second),
            Collections.singletonList(edge), false);

        Assertions.assertEquals(Arrays.asList("v1", "v2"), path.getVertexIds());
        Assertions.assertEquals(Collections.singletonList("edge-1"), path.getEdgeIds());
        Assertions.assertEquals(1, path.getHop());
        Assertions.assertEquals(first, path.getVertices().get(0));
        Assertions.assertTrue(path.sameIdentityAs(new GraphPathRef(
            Arrays.asList(first, second), Collections.singletonList(edge), false)));
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new GraphPathRef(Collections.singletonList(first),
                Collections.singletonList(edge), false));
    }

    @Test
    public void graphPathRejectsDisconnectedEdge() {
        GraphVertexRef first = vertex("v1", "e1");
        GraphVertexRef second = vertex("v2", "e2");
        GraphEdgeRef disconnected = edge("edge-1", "e1", "e3");
        Assertions.assertThrows(RetrievalModelValidationException.class,
            () -> new GraphPathRef(Arrays.asList(first, second),
                Collections.singletonList(disconnected), false));
    }

    @Test
    public void graphTraceDerivesSeedAndRoundTripsNormalizedJson() {
        GraphVertexRef seed = vertex("v1", "e1");
        GraphPathRef path = new GraphPathRef(Arrays.asList(seed, vertex("v2", "e2")),
            Collections.singletonList(edge("edge-1", "e1", "e2")), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("query-1", RetrievalMode.HYBRID,
            "ALIAS", 0.9, "graph-v1", path, Collections.singletonList("chunk-1"));

        Assertions.assertEquals(seed, trace.getSeed());
        Assertions.assertEquals("e1", trace.getAnchorEntityId());
        Assertions.assertEquals(trace,
            RetrievalModelJson.fromJson(RetrievalModelJson.toJson(trace), GraphEvidenceTrace.class));
    }

    @Test
    public void evidenceUsesSchemaVersionAndGraphTraces() {
        TextChunk chunk = new TextChunk("chunk-1", "doc-1", 0, 0, 4, 1,
            "Alice", "policy-v1", "sha256:chunk-1", "https://example/doc-1",
            "hotpotqa", "release-1", "dev", "sha256:document-1");
        SourceRef source = new SourceRef("doc-1", "https://example/doc-1", 0, 4,
            "hotpotqa", "release-1", "dev", "sha256:document-1");
        EntityRef entity = new EntityRef("e1", "Alice", Collections.emptyList(), "PERSON",
            Collections.singletonList("chunk-1"), "hotpotqa", "release-1", "dev",
            "sha256:document-1");
        GraphVertexRef seed = new GraphVertexRef("PERSON", "v1", "e1", "hotpotqa",
            "release-1", "dev", "sha256:document-1");
        GraphPathRef path = new GraphPathRef(Collections.singletonList(seed),
            Collections.emptyList(), false);
        GraphEvidenceTrace trace = new GraphEvidenceTrace("query-1", RetrievalMode.GRAPH_ONLY,
            "CANONICAL", 1.0, "graph-v1", path, Collections.singletonList("chunk-1"));
        Map<String, ChannelScore> scores = new HashMap<>();
        scores.put("bm25", new ChannelScore("bm25", 2.0, 0.8, 1));
        Evidence evidence = new Evidence("evidence-1", EvidenceKind.GRAPH_FACT, "Alice",
            Collections.singletonList(chunk), Collections.singletonList(entity),
            Collections.singletonList(source), scores, 0.8, null, 1,
            Collections.singletonList(trace), Evidence.CURRENT_SCHEMA_VERSION,
            "hotpotqa", "release-1", "dev", "graph-v1", "index-v1");

        Assertions.assertEquals(2, evidence.getSchemaVersion());
        Assertions.assertEquals(1, evidence.getGraphTraces().size());
        Assertions.assertEquals(path, evidence.getPaths().get(0));
        String json = RetrievalModelJson.toJson(evidence);
        Assertions.assertTrue(json.contains("\"graphTraces\""));
        Assertions.assertFalse(json.contains("\"paths\""));
        Assertions.assertEquals(evidence,
            RetrievalModelJson.fromJson(json, Evidence.class));
    }

    @Test
    public void evidenceJsonRequiresCurrentSchemaVersion() {
        Assertions.assertThrows(JsonParseException.class,
            () -> RetrievalModelJson.fromJson("{\"kind\":\"CHUNK\",\"text\":\"x\"}",
                Evidence.class));
        Assertions.assertThrows(JsonParseException.class,
            () -> RetrievalModelJson.fromJson("{\"kind\":\"CHUNK\",\"schemaVersion\":1}",
                Evidence.class));
    }

    @Test
    public void graphTraceJsonRejectsLegacyPathShape() {
        String json = "{\"queryId\":\"q\",\"selectedMode\":\"GRAPH_ONLY\","
            + "\"graphVersion\":\"v1\",\"anchorMatchType\":\"CANONICAL\","
            + "\"anchorConfidence\":1.0,\"path\":{\"vertexIds\":[\"v1\"],"
            + "\"edgeIds\":[],\"hop\":0,\"sampled\":false}}";
        Assertions.assertThrows(JsonParseException.class,
            () -> RetrievalModelJson.fromJson(json, GraphEvidenceTrace.class));
    }

    private static GraphVertexRef vertex(String vertexId, String entityId) {
        return new GraphVertexRef("PERSON", vertexId, entityId);
    }

    private static GraphEdgeRef edge(String edgeId, String source, String target) {
        return new GraphEdgeRef(edgeId, "knows", source, target);
    }
}
