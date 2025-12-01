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

package org.apache.geaflow.context.core.engine;

import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DefaultContextMemoryEngineTest {

    private DefaultContextMemoryEngine engine;
    private DefaultContextMemoryEngine.ContextMemoryConfig config;

    @Before
    public void setUp() throws Exception {
        config = new DefaultContextMemoryEngine.ContextMemoryConfig();
        engine = new DefaultContextMemoryEngine(config);
        engine.initialize();
    }

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    public void testEngineInitialization() {
        assertNotNull(engine);
        assertNotNull(engine.getEmbeddingIndex());
    }

    @Test
    public void testEpisodeIngestion() throws Exception {
        Episode episode = new Episode("ep_001", "Test Episode", System.currentTimeMillis(),
                "John knows Alice");

        Episode.Entity entity1 = new Episode.Entity("john", "John", "Person");
        Episode.Entity entity2 = new Episode.Entity("alice", "Alice", "Person");
        episode.setEntities(Arrays.asList(entity1, entity2));

        Episode.Relation relation = new Episode.Relation("john", "alice", "knows");
        episode.setRelations(Arrays.asList(relation));

        String episodeId = engine.ingestEpisode(episode);

        assertNotNull(episodeId);
        assertEquals("ep_001", episodeId);
    }

    @Test
    public void testSearch() throws Exception {
        // Ingest sample data
        Episode episode = new Episode("ep_001", "Test", System.currentTimeMillis(),
                "John is a software engineer");

        Episode.Entity entity = new Episode.Entity("john", "John", "Person");
        episode.setEntities(Arrays.asList(entity));

        engine.ingestEpisode(episode);

        // Test search
        ContextQuery query = new ContextQuery.Builder()
                .queryText("John")
                .strategy(ContextQuery.RetrievalStrategy.KEYWORD_ONLY)
                .build();

        ContextSearchResult result = engine.search(query);

        assertNotNull(result);
        assertTrue(result.getExecutionTime() > 0);
        // Should find John entity through keyword search
        assertTrue(result.getEntities().size() > 0);
    }

    @Test
    public void testEmbeddingIndex() throws Exception {
        ContextMemoryEngine.EmbeddingIndex index = engine.getEmbeddingIndex();

        float[] embedding = new float[]{0.1f, 0.2f, 0.3f};
        index.addEmbedding("entity_1", embedding);

        float[] retrieved = index.getEmbedding("entity_1");
        assertNotNull(retrieved);
        assertEquals(3, retrieved.length);
    }

    @Test
    public void testVectorSimilaritySearch() throws Exception {
        ContextMemoryEngine.EmbeddingIndex index = engine.getEmbeddingIndex();

        // Add some test embeddings
        float[] vec1 = new float[]{1.0f, 0.0f, 0.0f};
        float[] vec2 = new float[]{0.9f, 0.1f, 0.0f};
        float[] vec3 = new float[]{0.0f, 0.0f, 1.0f};

        index.addEmbedding("entity_1", vec1);
        index.addEmbedding("entity_2", vec2);
        index.addEmbedding("entity_3", vec3);

        // Search for similar vectors
        java.util.List<ContextMemoryEngine.EmbeddingSearchResult> results =
                index.search(vec1, 10, 0.5);

        assertNotNull(results);
        // Should find entity_1 and likely entity_2 (similar vectors)
        assertTrue(results.size() >= 1);
        assertEquals("entity_1", results.get(0).getEntityId());
    }

    @Test
    public void testContextSnapshot() throws Exception {
        Episode episode = new Episode("ep_001", "Test", System.currentTimeMillis(), "test");
        engine.ingestEpisode(episode);

        long timestamp = System.currentTimeMillis();
        ContextMemoryEngine.ContextSnapshot snapshot = engine.createSnapshot(timestamp);

        assertNotNull(snapshot);
        assertEquals(timestamp, snapshot.getTimestamp());
    }

    @Test
    public void testTemporalGraph() throws Exception {
        Episode episode = new Episode("ep_001", "Test", System.currentTimeMillis(), "test");
        engine.ingestEpisode(episode);

        ContextQuery.TemporalFilter filter = ContextQuery.TemporalFilter.last24Hours();
        ContextMemoryEngine.TemporalContextGraph graph = engine.getTemporalGraph(filter);

        assertNotNull(graph);
        assertTrue(graph.getStartTime() > 0);
        assertTrue(graph.getEndTime() > graph.getStartTime());
    }
}
