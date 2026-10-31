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

package org.apache.geaflow.ai.operator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The value filter feeding the embedding store must drop values that carry no meaning and keep the
 * ones that do.
 *
 * <p>It used to do the opposite, because the predicate returned on the first character
 * <em>inside</em> the ignorable set rather than the first one outside it. Ordinary prose was
 * therefore discarded and digit-only noise kept, and an embedding store silently produced nothing
 * for any text that happened to contain no digit: no request was issued, no error was raised, and
 * the entity was still registered as indexed with an empty vector list.
 */
public class IgnorableTextFilterTest {

    private static final String LABEL = "chunk";
    private static final String EDGE_LABEL = "rel";

    @Test
    public void testValuesCarryingMeaningAreNotIgnorable() {
        for (String value : new String[] {
            "the alpaca grazes on the hillside",
            "chunk 7 of the report",
            "a",
            "\u4e2d\u6587\u5185\u5bb9",
            "id-42 belongs to Alice"}) {
            Assertions.assertFalse(SearchUtils.isAllIgnorableChars(value),
                "value carries meaning and must be kept: " + value);
        }
    }

    @Test
    public void testValuesWithoutMeaningAreIgnorable() {
        for (String value : new String[] {
            "12345", "2024-01-01", "----", "___", "42.0", "%%", null, ""}) {
            Assertions.assertTrue(SearchUtils.isAllIgnorableChars(value),
                "value carries no meaning and must be dropped: " + value);
        }
    }

    /**
     * The property that matters, stated without reference to the implementation: whether a value is
     * kept must not depend on it containing a digit.
     */
    @Test
    public void testKeepingAValueDoesNotDependOnContainingADigit() {
        Assertions.assertEquals(
            SearchUtils.isAllIgnorableChars("the alpaca grazes"),
            SearchUtils.isAllIgnorableChars("the alpaca grazes 7"),
            "adding a digit to prose must not change whether it is indexable");
    }

    @Test
    public void testVerbalizationKeepsDigitFreeText() {
        LocalMemoryGraphAccessor accessor = buildGraph("no digits here at all", "still none");
        SubgraphSemanticPromptFunction func = new SubgraphSemanticPromptFunction(accessor);

        GraphEntity vertex = accessor.getVertex(LABEL, "v1");
        Assertions.assertEquals(Collections.singletonList("no digits here at all"),
            func.verbalize(vertex),
            "digit free vertex text must survive verbalization");

        List<GraphEntity> edges = new ArrayList<>(accessor.getEdge(EDGE_LABEL, "v1", "v2"));
        Assertions.assertEquals(1, edges.size());
        Assertions.assertEquals(Collections.singletonList("plain edge text"),
            func.verbalize(edges.get(0)),
            "digit free edge text must survive verbalization");
    }

    @Test
    public void testVerbalizationDropsValuesWithoutMeaning() {
        LocalMemoryGraphAccessor accessor = buildGraph("2024-01-01", "still none");
        SubgraphSemanticPromptFunction func = new SubgraphSemanticPromptFunction(accessor);
        Assertions.assertEquals(Collections.emptyList(),
            func.verbalize(accessor.getVertex(LABEL, "v1")),
            "a value that is only a date carries nothing to embed");
    }

    /**
     * End to end on the store, offline: an entity with digit free text must be queued for
     * embedding. The unusable model config makes that observable without a service, since reaching
     * the request at all fails on the null url.
     */
    @Test
    public void testStoreRequestsEmbeddingsForDigitFreeText(@TempDir Path tempDir) {
        LocalMemoryGraphAccessor accessor = buildGraph("no digits here at all", "still none");
        Path indexFile = tempDir.resolve("index.jsonl");
        EmbeddingIndexStore store = new EmbeddingIndexStore();

        int retries = Constants.MODEL_CLIENT_RETRY_TIMES;
        int interval = Constants.MODEL_CLIENT_RETRY_INTERVAL_MS;
        // One attempt is enough to show a request was made, and keeps the test off the retry budget.
        Constants.MODEL_CLIENT_RETRY_TIMES = 1;
        Constants.MODEL_CLIENT_RETRY_INTERVAL_MS = 1;
        try {
            Assertions.assertThrows(Throwable.class, () -> store.initStore(accessor,
                    new SubgraphSemanticPromptFunction(accessor), indexFile.toString(),
                    new ModelConfig(null, null, null, null)),
                "the store must try to embed this text; before the fix it silently did nothing");
        } finally {
            Constants.MODEL_CLIENT_RETRY_TIMES = retries;
            Constants.MODEL_CLIENT_RETRY_INTERVAL_MS = interval;
        }
    }

    /**
     * The counterpart: when nothing in the graph carries meaning there is genuinely nothing to
     * embed, so the store must complete without contacting a model.
     */
    @Test
    public void testStoreRequestsNothingWhenNoValueCarriesMeaning(@TempDir Path tempDir)
        throws Exception {
        // The edge value has to be ignorable as well, otherwise there is legitimately something
        // to embed and the store is right to try.
        LocalMemoryGraphAccessor accessor = buildGraph("2024-01-01", "1999", "42.0");
        Path indexFile = tempDir.resolve("index.jsonl");
        EmbeddingIndexStore store = new EmbeddingIndexStore();
        store.initStore(accessor, new SubgraphSemanticPromptFunction(accessor),
            indexFile.toString(), new ModelConfig(null, null, null, null));

        Assertions.assertEquals(Collections.emptyList(), Files.readAllLines(indexFile));
        Assertions.assertTrue(store.getEntityIndex(accessor.getVertex(LABEL, "v1")).isEmpty());
        Assertions.assertTrue(store.getEntityIndex(accessor.getVertex(LABEL, "v2")).isEmpty());
    }

    private LocalMemoryGraphAccessor buildGraph(String v1Text, String v2Text) {
        return buildGraph(v1Text, v2Text, "plain edge text");
    }

    private LocalMemoryGraphAccessor buildGraph(String v1Text, String v2Text, String edgeText) {
        GraphSchema schema = new GraphSchema();
        schema.setName("filter_graph");
        VertexSchema vs = new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        EdgeSchema es = new EdgeSchema(EDGE_LABEL, "srcId", "dstId",
            Collections.singletonList("rel"));
        schema.addVertex(vs);
        schema.addEdge(es);

        List<Vertex> vertices = new ArrayList<>(Arrays.asList(
            new Vertex(LABEL, "v1", Collections.singletonList(v1Text)),
            new Vertex(LABEL, "v2", Collections.singletonList(v2Text))));
        List<Edge> edges = new ArrayList<>(Collections.singletonList(
            new Edge(EDGE_LABEL, "v1", "v2", Collections.singletonList(edgeText))));
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vs, vertices));
        entities.put(EDGE_LABEL, new EdgeGroup(es, edges));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }
}
