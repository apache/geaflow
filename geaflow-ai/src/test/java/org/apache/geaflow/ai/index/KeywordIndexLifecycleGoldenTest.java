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

package org.apache.geaflow.ai.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphVertex;
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
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.operator.GraphSearchStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle and retrieval-semantics tests for the keyword index, pinning the behavior issue #856
 * calls out as fragile: the store is rebuilt during every query, retrieval tolerates common-token
 * noise, and deleted entities are filtered at query time rather than removed from the index.
 *
 * <p>The usage contract these tests document, in one place:
 *
 * <ul>
 *   <li><b>Build, close, search, discard.</b> A query session indexes what it wants searchable,
 *       closes the store (which commits the segments) and only then searches. Searching before
 *       close silently returns nothing — the {@code IndexNotFoundException} of the uncommitted
 *       directory is swallowed. Indexing again after close fails outright, because
 *       {@code SearchStore} reuses one {@code IndexWriterConfig} across writers. The store is
 *       therefore single-shot, and {@code SessionOperator} exercises exactly this order.</li>
 *   <li><b>Stop words are searchable.</b> The analyzer is built with an empty stop-word set
 *       (see {@link KeywordTokenizationGoldenTest}), so {@code the} matches documents
 *       containing {@code the}.</li>
 *   <li><b>Common-token noise does not crowd out exact anchoring.</b> Every document carries the
 *       tokens of the {@code KeywordVector{vec=[...]} toString()} wrapper, so a query for that
 *       wrapper text matches everything up to the TopN cap. A query for a term that actually
 *       distinguishes documents still returns exactly the documents holding it, even when the
 *       corpus is larger than TopN.</li>
 *   <li><b>Deletion is a query-time filter.</b> Nothing is ever removed from the Lucene index;
 *       an indexed entity that no longer resolves through the {@code GraphAccessor} (deleted, or
 *       never inserted, or carrying an unregistered label) simply does not surface in results.</li>
 *   <li><b>Multi-word queries are OR queries.</b> A document containing any queried term is
 *       returned, ranked by score.</li>
 * </ul>
 */
public class KeywordIndexLifecycleGoldenTest {

    private static final String LABEL = "chunk";
    private static final String EDGE_LABEL = "rel";

    /** The production order: index, close, search. Everything visible before close is found. */
    @Test
    public void indexedEntitiesAreSearchableAfterClose() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca", "grazes");
        store.close();

        List<String> found = searchIds(store, accessor, "alpaca");
        Assertions.assertEquals(Collections.singletonList("v1"), found,
            "a term indexed before close must be found after close");
        Assertions.assertEquals(Collections.singletonList("v1"), searchIds(store, accessor, "ALPACA"),
            "matching is case-insensitive on both sides of the index");
        Assertions.assertEquals(Collections.singletonList("v1"), searchIds(store, accessor, "grazes"),
            "every keyword of a vector is independently searchable");
        Assertions.assertTrue(searchIds(store, accessor, "lama").isEmpty(),
            "a term indexed by no entity matches nothing");
    }

    /**
     * Searching before close returns an empty list instead of failing. This is why the
     * close-before-search order above is a contract rather than a convention: getting it wrong
     * fails silently.
     */
    @Test
    public void searchBeforeCloseReturnsEmptyResults() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        Assertions.assertEquals(Collections.emptyList(), searchIds(store, accessor, "alpaca"),
            "search before close must return empty, not the committed documents (there are none)");
    }

    /**
     * Indexing after close throws: {@code SearchStore} hands the same {@code IndexWriterConfig}
     * to a second {@code IndexWriter} and Lucene rejects that. Combined with the test above this
     * fixes the lifecycle as index-then-close-then-search with no reopen — the resident index of
     * PR #825 is the proposed way out of this single-shot shape.
     */
    @Test
    public void indexingAfterCloseFails() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        store.close();
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
            () -> indexVertex(store, "v2", "lama"),
            "indexing into a closed store must fail loudly rather than lose the document");
        Assertions.assertTrue(failure.getCause() instanceof IllegalStateException,
            "the underlying refusal is Lucene's IndexWriterConfig sharing guard, got: "
                + failure.getCause());
    }

    /** An empty query reaches the query parser and surfaces as the store's read failure. */
    @Test
    public void emptyQueryFailsTheSearchStore() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        store.close();
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
            () -> store.search("", accessor));
        Assertions.assertEquals("Cannot read search store", failure.getMessage());
    }

    /**
     * Multi-word queries are parsed with the default OR: a document holding just one of the
     * queried terms is a hit. formatQuery turns {@code alpaca.lama} into the two terms
     * {@code alpaca lama}, and the document holding only {@code alpaca} still matches.
     */
    @Test
    public void multiWordQueriesMatchAnyTerm() {
        LocalMemoryGraphAccessor accessor = graph("v1", "v2");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        indexVertex(store, "v2", "grazes");
        store.close();

        Assertions.assertEquals(Collections.singletonList("v1"), searchIds(store, accessor, "alpaca.lama"),
            "OR semantics: only one of the two terms needs to be present");
    }

    /** Stop words are ordinary searchable terms (the analyzer has no stop-word list). */
    @Test
    public void stopWordsAreSearchableTerms() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "the", "fox");
        store.close();
        Assertions.assertEquals(Collections.singletonList("v1"), searchIds(store, accessor, "the"),
            "with an empty stop-word set, 'the' indexes and matches like any other word");
    }

    /**
     * CJK keywords are indexed as single characters and matched through OR, so a two-character
     * query is an OR of its characters. An exact keyword matches its documents — and a document
     * sharing only one character of the query matches too: querying {@code 学习} also returns the
     * document indexed under {@code 习题}, through the shared character {@code 习}. That is the
     * current recall behavior for CJK, recorded here so a change (bigram tokenization, AND
     * semantics) is a visible decision instead of a silent drift.
     */
    @Test
    public void unicodeKeywordsMatchByCharacter() {
        LocalMemoryGraphAccessor accessor = graph("v1", "v2");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "学习", "效率");
        indexVertex(store, "v2", "习题", "效率");
        store.close();

        Assertions.assertEquals(Arrays.asList("v1", "v2"), searchIds(store, accessor, "效率"),
            "an exact two-character keyword matches the documents carrying it");
        Assertions.assertEquals(Arrays.asList("v1", "v2"), searchIds(store, accessor, "学习"),
            "OR of single characters: a document sharing one character of the query also matches");
    }

    /**
     * The anchor of issue #856's acceptance criteria. Every document contains the wrapper tokens
     * {@code keywordvector} and {@code vec} from {@code KeywordVector.toString()}, on a corpus
     * deliberately larger than the TopN cap of {@code GRAPH_SEARCH_STORE_DEFAULT_TOPN}. Two
     * properties must hold:
     *
     * <ul>
     *   <li>a distinctive term returns exactly its own document — the common tokens shared by all
     *       documents do not crowd it out of the TopN window;</li>
     *   <li>querying the wrapper text itself demonstrates the noise: every document matches and
     *       the result is capped at TopN.</li>
     * </ul>
     */
    @Test
    public void commonWrapperTokensDoNotCrowdOutExactAnchoring() {
        int corpusSize = Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN + 10;
        String[] ids = new String[corpusSize];
        for (int i = 0; i < corpusSize; i++) {
            ids[i] = "v" + i;
        }
        LocalMemoryGraphAccessor accessor = graph(ids);

        GraphSearchStore store = new GraphSearchStore();
        for (int i = 0; i < corpusSize; i++) {
            // One shared filler keyword plus a unique one: every document overlaps its neighbors,
            // none of them is distinguishable through the filler alone.
            indexVertex(store, ids[i], "shared", "unique" + i);
        }
        store.close();

        Assertions.assertEquals(Collections.singletonList("v17"),
            searchIds(store, accessor, "unique17"),
            "an exact term must return exactly its document, common tokens must not crowd it out");
        Assertions.assertEquals(Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN,
            searchIds(store, accessor, "shared").size(),
            "a term held by every document matches the whole corpus and is capped at TopN");
        Assertions.assertEquals(Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN,
            searchIds(store, accessor, "keywordvector").size(),
            "querying the toString() wrapper text matches every document too — this is the "
                + "common-token pollution a dedicated query representation would remove");
    }

    /**
     * Incremental addition is accumulation within one open session: documents added in several
     * batches are all committed by the single close and all searchable. There is no incremental
     * visibility into an already-searched index (see {@link #indexingAfterCloseFails()}).
     */
    @Test
    public void incrementalAddsAccumulateUntilClose() {
        LocalMemoryGraphAccessor accessor = graph("v1", "v2", "v3");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        indexVertex(store, "v2", "lama");
        indexVertex(store, "v3", "zebra");
        store.close();

        Assertions.assertEquals(Arrays.asList("v1", "v2", "v3"), searchIds(store, accessor, "alpaca lama zebra"),
            "every batch added before close is committed and searchable");
    }

    /**
     * Deletion is not an index operation: the Lucene document survives, but results resolve every
     * hit through the {@code GraphAccessor} and drop what the graph no longer (or never) held.
     * The same filter drops documents carrying a label the graph schema does not register.
     */
    @Test
    public void deletedEntitiesAreFilteredAtQueryTime() {
        LocalMemoryGraphAccessor accessor = graph("v1");
        GraphSearchStore store = new GraphSearchStore();
        indexVertex(store, "v1", "alpaca");
        // A vertex indexed with a registered label but an id the graph does not hold — the shape
        // a deleted entity leaves behind.
        indexVertex(store, "ghost", "ghostword");
        // A vertex whose label is not in the schema at all.
        store.indexVertex(new GraphVertex(
                new Vertex("unknown", "v9", Collections.singletonList("x"))),
            Collections.singletonList(new KeywordVector("orphanword")));
        store.close();

        Assertions.assertEquals(Collections.singletonList("v1"), searchIds(store, accessor, "alpaca"));
        Assertions.assertTrue(searchIds(store, accessor, "ghostword").isEmpty(),
            "a hit whose entity is gone from the graph must not surface");
        Assertions.assertTrue(searchIds(store, accessor, "orphanword").isEmpty(),
            "a hit whose label is not registered in the schema must not surface");
    }

    // ------------------------------------------------------------------ helpers

    private LocalMemoryGraphAccessor graph(String... ids) {
        GraphSchema schema = new GraphSchema();
        schema.setName("keyword-golden");
        VertexSchema vs = new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        EdgeSchema es = new EdgeSchema(EDGE_LABEL, "srcId", "dstId",
            Collections.singletonList("rel"));
        schema.addVertex(vs);
        schema.addEdge(es);
        List<Vertex> vertices = new ArrayList<>();
        for (String id : ids) {
            vertices.add(new Vertex(LABEL, id, Collections.singletonList("text of " + id)));
        }
        List<Edge> edges = Collections.emptyList();
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vs, vertices));
        entities.put(EDGE_LABEL, new EdgeGroup(es, edges));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    private void indexVertex(GraphSearchStore store, String id, String... keywords) {
        store.indexVertex(new GraphVertex(
                new Vertex(LABEL, id, Collections.singletonList("text of " + id))),
            Collections.singletonList(new KeywordVector(keywords)));
    }

    private List<String> searchIds(GraphSearchStore store, LocalMemoryGraphAccessor accessor,
                                   String query) {
        List<String> ids = new ArrayList<>();
        for (org.apache.geaflow.ai.graph.GraphEntity entity : store.search(query, accessor)) {
            ids.add(((GraphVertex) entity).getVertex().getId());
        }
        return ids;
    }
}
