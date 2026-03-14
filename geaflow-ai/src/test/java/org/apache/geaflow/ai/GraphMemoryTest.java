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

package org.apache.geaflow.ai;

import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.graph.EmptyGraphAccessor;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.index.vector.MagnitudeVector;
import org.apache.geaflow.ai.index.vector.TraversalVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class GraphMemoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphMemoryTest.class);

    @Test
    public void testVectorSearch() {
        VectorSearch search = new VectorSearch(null, "test01");
        search.addVector(new EmbeddingVector(new double[0]));
        search.addVector(new KeywordVector("test1", "test2"));
        search.addVector(new MagnitudeVector());
        search.addVector(new TraversalVector("src", "edge", "dst"));
        LOGGER.info(String.valueOf(search));
    }

    // ========== MagnitudeVector Tests ==========
    
    @Test
    public void testMagnitudeVectorConstructorAndGetter() {
        MagnitudeVector vector = new MagnitudeVector(0.85);
        assertEquals(vector.getMagnitude(), 0.85, 0.0001);
    }

    @Test
    public void testMagnitudeVectorMatchExactSameValue() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);

        assertEquals(v1.match(v2), 1.0, 0.0001);
    }

    @Test
    public void testMagnitudeVectorMatchDifferentValues() {
        MagnitudeVector v1 = new MagnitudeVector(10.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);

        // Expected: 1 - |10-5|/max(10,5) = 1 - 5/10 = 0.5
        assertEquals(v1.match(v2), 0.5, 0.0001);
    }

    @Test
    public void testMagnitudeVectorMatchWithIncompatibleType() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        IVector incompatibleVector = new IVector() {
            @Override
            public double match(IVector other) {
                return 0;
            }

            @Override
            public VectorType getType() {
                return null;
            }
        };

        assertEquals(v1.match(incompatibleVector), 0.0, 0.0001);
    }

    @Test
    public void testMagnitudeVectorEqualsAndHashCode() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);
        MagnitudeVector v3 = new MagnitudeVector(10.0);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertNotEquals(v1, v3);
    }

    @Test
    public void testMagnitudeVectorToString() {
        MagnitudeVector vector = new MagnitudeVector(0.75);
        String str = vector.toString();

        assertEquals(str, "MagnitudeVector{magnitude=0.75}");
    }

    @Test
    public void testMagnitudeVectorGetType() {
        MagnitudeVector vector = new MagnitudeVector(1.0);
        assertEquals(vector.getType(), VectorType.MagnitudeVector);
    }

    // ========== TraversalVector Tests ==========
    
    @Test
    public void testTraversalVectorConstructorValidInput() {
        new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );

        // Should not throw exception
    }

    @Test
    public void testTraversalVectorConstructorInvalidInput() {
        // Should throw exception if not multiple of 3
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () ->
            new TraversalVector("Alice", "knows", "Bob", "Bob", "knows")
        );
    }

    @Test
    public void testTraversalVectorMatchExactSamePath() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );

        assertEquals(v1.match(v2), 1.0, 0.0001);
    }

    @Test
    public void testTraversalVectorMatchSubgraphContainment() {
        // v1 is contained within v2
        TraversalVector v1 = new TraversalVector(
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie",
            "Charlie", "knows", "Dave"
        );

        // v1 is subgraph of v2, so score should be 0.8
        assertEquals(v1.match(v2), 0.8, 0.0001);
    }

    @Test
    public void testTraversalVectorMatchPartialOverlap() {
        // Two vectors sharing one common edge
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "likes", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Bob", "knows", "Charlie",
            "Alice", "knows", "Bob"
        );

        // One common edge out of 3 unique edges total = 1/3
        double expected = 1.0 / 3.0;
        assertEquals(v1.match(v2), expected, 0.0001);
    }

    @Test
    public void testTraversalVectorMatchNoOverlap() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob"
        );
        TraversalVector v2 = new TraversalVector(
            "Charlie", "knows", "Dave"
        );

        assertEquals(v1.match(v2), 0.0, 0.0001);
    }

    @Test
    public void testTraversalVectorEqualsAndHashCode() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v3 = new TraversalVector(
            "Bob", "knows", "Charlie"
        );

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertNotEquals(v1, v3);
    }

    @Test
    public void testTraversalVectorToString() {
        TraversalVector vector = new TraversalVector(
            "Alice", "knows", "Bob"
        );
        String str = vector.toString();

        assertEquals(str, "TraversalVector{vec=Alice-knows-Bob}");
    }

    @Test
    public void testTraversalVectorGetType() {
        TraversalVector vector = new TraversalVector("Alice", "knows", "Bob");
        assertEquals(vector.getType(), VectorType.TraversalVector);
    }

    @Test
    public void testEmptyMainPipeline() {
        GraphMemoryServer server = new GraphMemoryServer();
        IndexStore indexStore = new EntityAttributeIndexStore();
        GraphAccessor graphAccessor = new EmptyGraphAccessor();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);

        String sessionId = server.createSession();
        VectorSearch search = new VectorSearch(null, sessionId);
        String context = produceCycle(server, search, graphAccessor);
        Assertions.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search, graphAccessor);
        Assertions.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search, graphAccessor);
        Assertions.assertEquals(context, "Context{prompt=''}");
    }

    private String produceCycle(GraphMemoryServer server, VectorSearch search,
                                GraphAccessor graphAccessor) {
        String sessionId = search.getSessionId();
        String searchResult = server.search(search);
        Assertions.assertNotNull(searchResult);
        Context context = server.verbalize(sessionId,
                new SubgraphSemanticPromptFunction(graphAccessor));
        Assertions.assertNotNull(context);
        return context.toString();
    }

    @Test
    public void testLdbcMainPipeline() {
        LdbcPromptFormatter ldbcPromptFormatter = new LdbcPromptFormatter();
        LocalMemoryGraphAccessor graphAccessor =
                new LocalMemoryGraphAccessor(this.getClass().getClassLoader(),
                        "graph_ldbc_sf", 7500L,
                        ldbcPromptFormatter::vertexMapper, ldbcPromptFormatter::edgeMapper);
        graphAccessor.getGraphSchema().setPromptFormatter(ldbcPromptFormatter);
        LOGGER.info("Success to init graph data.");

        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(graphAccessor));
        LOGGER.info("Success to init EntityAttributeIndexStore.");

        ModelConfig modelInfo = new ModelConfig(null, null, null, null);
        EmbeddingIndexStore embeddingStore = new EmbeddingIndexStore();
        embeddingStore.initStore(graphAccessor,
            new SubgraphSemanticPromptFunction(graphAccessor),
            "src/test/resources/index/LDBCEmbeddingIndexStore",
            modelInfo);
        LOGGER.info("Success to init EmbeddingIndexStore.");

        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        server.addIndexStore(embeddingStore);
        MockChatRobot robot = new MockChatRobot();
        robot.setModelInfo(modelInfo);

        {
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            String query = "What comments has Chaim Azriel posted?";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));
            LOGGER.info("Round 1: \n" + search);

            String context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 1: \n" + context);
            Assertions.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is Person166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
            Assertions.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is Person21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

            search = new VectorSearch(null, sessionId);
            query = "Chaim Azriel, Comment_hasCreator_Person, personId, comment author, Person166, Person21990232556059";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));
            LOGGER.info("Round 2: \n" + search);

            context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 2: \n" + context);
            Assertions.assertTrue(context.contains("Comment824633737550"));
            Assertions.assertTrue(context.contains("Comment962072691263"));
            Assertions.assertTrue(context.contains("Comment687194784946"));
        }

        {
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            String query = "How many posts has Chaim Azriel posted?";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));

            LOGGER.info("Round 1: \n" + search);

            String context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 1: \n" + context);
            Assertions.assertTrue(context.contains("A Forum, name is Wall of Chaim Azriel Hleb, id is Forum220, created at 2010-02-19T12:42:24.255+00:00"));
            Assertions.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is Person166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
            Assertions.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is Person21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

            search = new VectorSearch(null, sessionId);
            query = "Chaim Azriel, Post_hasCreator_Person, Post";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));
            LOGGER.info("Round 2: \n" + search);

            context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 2: \n" + context);
            Assertions.assertTrue(context.contains("Post755914247530"));
            Assertions.assertTrue(context.contains("Post1099511644342"));
            Assertions.assertTrue(context.contains("Person166"));
        }

        {
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            String query = "Which historical comments exist, who posted them, and how many have been published?";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));

            LOGGER.info("Round 1: \n" + search);

            String context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 1: \n" + context);
            Assertions.assertTrue(context.contains("Comment1099511634167"));
            Assertions.assertTrue(context.contains("Comment1030792164752"));
            Assertions.assertTrue(context.contains("Comment1099511645848"));

            search = new VectorSearch(null, sessionId);
            query = "Comment_hasCreator_Person, Person, Comment IDs, Comment1030792157359";
            search.addVector(new KeywordVector(query));
            search.addVector(new EmbeddingVector(robot.embeddingSingle(query).embedding));
            LOGGER.info("Round 2: \n" + search);

            context = produceCycle(server, search, graphAccessor);
            LOGGER.info("Round 2: \n" + context);
            Assertions.assertTrue(context.contains("Person24189255812253"));
            Assertions.assertTrue(context.contains("Person26388279067480"));
        }
    }
}
