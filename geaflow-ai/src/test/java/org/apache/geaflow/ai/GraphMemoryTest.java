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
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.index.vector.MagnitudeVector;
import org.apache.geaflow.ai.index.vector.TraversalVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
