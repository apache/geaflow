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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.VertexVersionWindow;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The entity key is used as the Lucene primary key, so update and delete by that term must affect
 * exactly one entity. Plainly concatenating id and label is not injective: {@code (id="a",
 * label="bc")} and {@code (id="ab", label="c")} both yield {@code Vabc}, and one entity's write
 * would then destroy the other's document.
 */
public class EntityKeyCollisionTest {

    /** Two labels whose concatenation with the ids below used to produce the same key. */
    private static final String LABEL_LONG = "bc";
    private static final String LABEL_SHORT = "c";
    private static final String EDGE_LABEL_LONG = "re";
    private static final String EDGE_LABEL_SHORT = "e";

    private LocalMemoryGraphAccessor buildGraph() {
        GraphSchema schema = new GraphSchema();
        schema.setName("collision_graph");
        VertexSchema longLabel = new VertexSchema(LABEL_LONG, "id", Collections.singletonList("text"));
        VertexSchema shortLabel = new VertexSchema(LABEL_SHORT, "id", Collections.singletonList("text"));
        schema.addVertex(longLabel);
        schema.addVertex(shortLabel);

        Map<String, EntityGroup> entities = new HashMap<>();
        // "a" + "bc" and "ab" + "c" both concatenate to "abc".
        entities.put(LABEL_LONG, new VertexGroup(longLabel, new ArrayList<>(Collections.singletonList(
            new Vertex(LABEL_LONG, "a", Collections.singletonList("alpaca lives here"))))));
        entities.put(LABEL_SHORT, new VertexGroup(shortLabel, new ArrayList<>(Collections.singletonList(
            new Vertex(LABEL_SHORT, "ab", Collections.singletonList("bonobo lives here"))))));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    private EntityAttributeIndexStore newIndexStore(LocalMemoryGraphAccessor accessor) {
        EntityAttributeIndexStore store = new EntityAttributeIndexStore();
        store.initStore(new SubgraphSemanticPromptFunction(accessor));
        return store;
    }

    private static Set<String> idsOf(List<GraphEntity> entities) {
        Set<String> ids = new HashSet<>();
        for (GraphEntity entity : entities) {
            ids.add(((GraphVertex) entity).getVertex().getId());
        }
        return ids;
    }

    @Test
    public void testVertexKeysDoNotCollide() {
        GraphVertex first = new GraphVertex(new Vertex(LABEL_LONG, "a", Collections.emptyList()));
        GraphVertex second = new GraphVertex(new Vertex(LABEL_SHORT, "ab", Collections.emptyList()));
        Assertions.assertEquals(ModelUtils.getLegacyGraphEntityKey(first),
            ModelUtils.getLegacyGraphEntityKey(second),
            "this pair must be a collision under the legacy encoding, or the test proves nothing");
        Assertions.assertNotEquals(ModelUtils.getGraphEntityKey(first),
            ModelUtils.getGraphEntityKey(second));
    }

    @Test
    public void testEdgeKeysDoNotCollide() {
        // src + label + dst: "a" + "re" + "b" and "ar" + "e" + "b" both concatenate to "areb".
        GraphEdge first = new GraphEdge(
            new Edge(EDGE_LABEL_LONG, "a", "b", Collections.emptyList()));
        GraphEdge second = new GraphEdge(
            new Edge(EDGE_LABEL_SHORT, "ar", "b", Collections.emptyList()));
        Assertions.assertEquals(ModelUtils.getLegacyGraphEntityKey(first),
            ModelUtils.getLegacyGraphEntityKey(second),
            "this pair must be a collision under the legacy encoding, or the test proves nothing");
        Assertions.assertNotEquals(ModelUtils.getGraphEntityKey(first),
            ModelUtils.getGraphEntityKey(second));
    }

    @Test
    public void testKeyIsInjectiveAcrossManySplits() {
        // Every way of splitting one string into id and label must give a distinct key.
        String joined = "abcd";
        Set<String> keys = new HashSet<>();
        for (int split = 1; split < joined.length(); split++) {
            GraphVertex vertex = new GraphVertex(new Vertex(joined.substring(split),
                joined.substring(0, split), Collections.emptyList()));
            Assertions.assertTrue(keys.add(ModelUtils.getGraphEntityKey(vertex)),
                "duplicate key for id=" + joined.substring(0, split)
                    + " label=" + joined.substring(split));
        }
        Assertions.assertEquals(joined.length() - 1, keys.size());
    }

    @Test
    public void testUpdatingOneEntityKeepsTheCollidingOneSearchable() {
        LocalMemoryGraphAccessor accessor = buildGraph();
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex index = new ResidentSearchIndex();
        index.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(2, index.getIndexedEntityNum());
        Assertions.assertEquals(Collections.singleton("a"),
            idsOf(index.search("alpaca", accessor)));
        Assertions.assertEquals(Collections.singleton("ab"),
            idsOf(index.search("bonobo", accessor)));

        Vertex updated = new Vertex(LABEL_LONG, "a", Collections.singletonList("caribou now"));
        VertexVersionWindow window = VertexVersionWindow.open(accessor);
        Assertions.assertEquals(0, accessor.getMutableGraph().updateVertex(updated));
        index.onEntitiesUpserted(accessor, entities(updated), store, window.seal());

        Assertions.assertEquals(Collections.singleton("a"),
            idsOf(index.search("caribou", accessor)));
        Assertions.assertEquals(Collections.singleton("ab"),
            idsOf(index.search("bonobo", accessor)),
            "updating one entity must not delete the document of a legacy key collision partner");
        Assertions.assertEquals(2, index.getIndexedEntityNum());
        Assertions.assertEquals(1L, index.getBuildCount());
    }

    @Test
    public void testDeletingOneEntityKeepsTheCollidingOneSearchable() {
        LocalMemoryGraphAccessor accessor = buildGraph();
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex index = new ResidentSearchIndex();
        index.ensureGlobalIndex(accessor, store);

        Vertex removed = accessor.getVertex(LABEL_LONG, "a").getVertex();
        VertexVersionWindow window = VertexVersionWindow.open(accessor);
        Assertions.assertEquals(0, accessor.getMutableGraph().removeVertex(LABEL_LONG, "a"));
        index.onEntitiesRemoved(accessor, entities(removed), store, window.seal());

        Assertions.assertTrue(index.search("alpaca", accessor).isEmpty());
        Assertions.assertEquals(Collections.singleton("ab"),
            idsOf(index.search("bonobo", accessor)),
            "deleting one entity must not delete the document of a legacy key collision partner");
        Assertions.assertEquals(1, index.getIndexedEntityNum());
        Assertions.assertEquals(1L, index.getBuildCount());
    }

    @Test
    public void testEdgeSchemaKeysDoNotCollideInTheStore() {
        // Guards the edge branch of the key through the store rather than through ModelUtils alone.
        LocalMemoryGraphAccessor accessor = buildGraph();
        Assertions.assertEquals(0, accessor.getMutableGraph().addEdgeSchema(
            new EdgeSchema(EDGE_LABEL_LONG, "srcId", "dstId", Collections.singletonList("rel"))));
        Assertions.assertEquals(0, accessor.getMutableGraph().addEdgeSchema(
            new EdgeSchema(EDGE_LABEL_SHORT, "srcId", "dstId", Collections.singletonList("rel"))));

        GraphSearchStore searchStore = new GraphSearchStore();
        GraphEdge first = new GraphEdge(new Edge(EDGE_LABEL_LONG, "a", "b",
            Collections.singletonList("first")));
        GraphEdge second = new GraphEdge(new Edge(EDGE_LABEL_SHORT, "ar", "b",
            Collections.singletonList("second")));
        searchStore.upsertEdge(first, Collections.singletonList(new KeywordVector("dingo")));
        searchStore.upsertEdge(second, Collections.singletonList(new KeywordVector("emu")));
        searchStore.refresh();
        Assertions.assertEquals(2, searchStore.getDocNum());

        searchStore.removeEntity(first);
        searchStore.refresh();
        Assertions.assertEquals(1, searchStore.getDocNum(),
            "removing one edge must not remove a colliding one");
        searchStore.close();
    }

    /**
     * An index file written before the key encoding was fixed is still readable, but only where the
     * legacy key identifies one entity. Where two entities share it, the entry must be dropped
     * rather than attached to whichever entity happens to win, which would hand one entity another
     * one's vectors.
     *
     * <p>The vertex texts are letters only on purpose: the verbalization
     * {@code SubgraphSemanticPromptFunction} feeds to the embedding store filters out values that
     * contain no digit or punctuation, so nothing is left to embed and the store needs no model.
     * The assertion on the file length below fails if that ever stops holding.
     */
    @Test
    public void testAmbiguousLegacyKeysAreDroppedNotGuessed(@TempDir Path tempDir) throws Exception {
        LocalMemoryGraphAccessor accessor = buildGraph();
        // A third vertex whose legacy key is unique, as the control.
        accessor.getMutableGraph().addVertex(
            new Vertex(LABEL_SHORT, "zz", Collections.singletonList("dingo lives here")));

        GraphVertex colliding = accessor.getVertex(LABEL_LONG, "a");
        GraphVertex partner = accessor.getVertex(LABEL_SHORT, "ab");
        GraphVertex unique = accessor.getVertex(LABEL_SHORT, "zz");
        String sharedLegacyKey = ModelUtils.getLegacyGraphEntityKey(colliding);
        Assertions.assertEquals(sharedLegacyKey, ModelUtils.getLegacyGraphEntityKey(partner));
        Assertions.assertNotEquals(sharedLegacyKey, ModelUtils.getLegacyGraphEntityKey(unique));

        Path indexFile = tempDir.resolve("legacy-index.jsonl");
        List<String> lines = Arrays.asList(
            "{\"input\":\"" + sharedLegacyKey + "\",\"embedding\":[1.0,0.0]}",
            "{\"input\":\"" + ModelUtils.getLegacyGraphEntityKey(unique) + "\",\"embedding\":[0.0,1.0]}");
        Files.write(indexFile, lines);

        EmbeddingIndexStore store = new EmbeddingIndexStore();
        store.initStore(accessor, new SubgraphSemanticPromptFunction(accessor),
            indexFile.toString(), new ModelConfig(null, null, null, null));

        Assertions.assertEquals(2, Files.readAllLines(indexFile).size(),
            "no embedding may have been requested, or this test is talking to a model");
        Assertions.assertTrue(store.getEntityIndex(colliding).isEmpty(),
            "an ambiguous legacy key must not be resolved to one of the candidates");
        Assertions.assertTrue(store.getEntityIndex(partner).isEmpty(),
            "an ambiguous legacy key must not be resolved to one of the candidates");
        Assertions.assertEquals(1, store.getEntityIndex(unique).size(),
            "an unambiguous legacy key must still be matched");
    }

    private static List<GraphEntity> entities(Vertex... vertices) {
        List<GraphEntity> list = new ArrayList<>(vertices.length);
        for (Vertex vertex : vertices) {
            list.add(new GraphVertex(vertex));
        }
        return list;
    }
}
