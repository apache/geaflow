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

import org.apache.geaflow.ai.graph.EmptyGraphAccessor;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphAccessorFactory;
import org.apache.geaflow.ai.graph.LocalFileGraphAccessor;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.index.vector.MagnitudeVector;
import org.apache.geaflow.ai.index.vector.TraversalVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphMemoryTest {

    @Test
    public void testVectorSearch() {
        VectorSearch search = new VectorSearch(null, "test01");
        search.addVector(new EmbeddingVector(new double[0]));
        search.addVector(new KeywordVector("test1", "test2"));
        search.addVector(new MagnitudeVector());
        search.addVector(new TraversalVector("src", "edge", "dst"));
        System.out.println(search);
    }

    @Test
    public void testEmptyMainPipeline() {
        GraphMemoryServer server =  new GraphMemoryServer();
        //创建图索引
        IndexStore indexStore = new EntityAttributeIndexStore();
        //加载图数据，加载索引数据
        GraphAccessor graphAccessor = new EmptyGraphAccessor();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);

        //创建会话
        String sessionId = server.createSession();
        //创建图检索
        VectorSearch search = new VectorSearch(null, sessionId);
        //进行图检索
        String context = produceCycle(server, search, graphAccessor);
        Assert.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search, graphAccessor);
        Assert.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search, graphAccessor);
        Assert.assertEquals(context, "Context{prompt=''}");
    }

    private String produceCycle(GraphMemoryServer server, VectorSearch search,
                                GraphAccessor graphAccessor) {
        String sessionId = search.getSessionId();
        String searchResult = server.search(search);
        Assert.assertNotNull(searchResult);
        Context context = server.verbalize(sessionId,
                new SubgraphSemanticPromptFunction(graphAccessor));
        Assert.assertNotNull(context);
        return context.toString();
    }

    @Test
    public void testLdbcMainPipeline() {
        LocalFileGraphAccessor graphAccessor =
                new LocalFileGraphAccessor(this.getClass().getClassLoader(),
                        "org/apache/geaflow/ai/graph_ldbc_sf", 10000L);
        graphAccessor.getGraphSchema().setPromptFormatter(new LdbcPromptFormatter());
        GraphAccessorFactory.INSTANCE = graphAccessor;
        IndexStore indexStore = new EntityAttributeIndexStore();

        GraphMemoryServer server =  new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        {
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector("What comments has Chaim Azriel posted?"));
            System.out.println("Round 1: \n" + search);

            String context = produceCycle(server, search, graphAccessor);
            System.out.println("Round 1: \n" + context);
            Assert.assertTrue(context.contains("A Forum, name is Wall of Chaim Azriel Hleb, id is 220, created at 2010-02-19T12:42:24.255+00:00"));
            Assert.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is 166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
            Assert.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is 21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

            search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector("Chaim Azriel, Comment_hasCreator_Person, personId, comment author, 166, 21990232556059"));
            System.out.println("Round 2: \n" + search);

            context = produceCycle(server, search, graphAccessor);
            System.out.println("Round 2: \n" + context);
            Assert.assertTrue(context.contains("A comment, id is 824633737550, created at 2012-02-10T11:09:12.368+00:00, user ip is 80.94.167.126, user use browser is Firefox, content is fine"));
            Assert.assertTrue(context.contains("A comment, id is 962072691263, created at 2012-06-09T16:26:47.659+00:00, user ip is 80.94.167.126, user use browser is Firefox, content is About Pope John Paul II, ps, and ordained many priests. A key goal of his papac"));
            Assert.assertTrue(context.contains("A comment, id is 687194784946, created at 2011-09-22T21:14:33.539+00:00, user ip is 80.94.167.126, user use browser is Firefox, content is About Mohammad Reza Pahlavi, e Persian Empire by Cyrus the Great. The About Quee"));
        }

        {
            String sessionId = server.createSession();
            VectorSearch search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector("How many posts has Chaim Azriel posted?"));
            System.out.println("Round 1: \n" + search);

            String context = produceCycle(server, search, graphAccessor);
            System.out.println("Round 1: \n" + context);
            Assert.assertTrue(context.contains("A Forum, name is Wall of Chaim Azriel Hleb, id is 220, created at 2010-02-19T12:42:24.255+00:00"));
            Assert.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is 166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
            Assert.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is 21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

            search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector("Chaim Azriel, Post_hasCreator_Person, Post"));
            System.out.println("Round 2: \n" + search);

            context = produceCycle(server, search, graphAccessor);
            System.out.println("Round 2: \n" + context);
            Assert.assertTrue(context.contains("A post, id is 755914247530, created at 2011-12-21T12:05:43.074+00:00, user ip is 178.238.2.172, user use browser is Firefox, use language is pl, content is About Louis XIV of France, cine, Boileau, La Fontaine, LullAbout Richard II of England, end o"));
            Assert.assertTrue(context.contains("A post, id is 1099511644342, created at 2012-10-16T09:44:45.548+00:00, user ip is 80.94.167.126, user use browser is Firefox, use language is gu, content is About Mohammad Reza Pahlavi, re enacted, including the banning of the communist Tudeh Party, and a gener"));
            Assert.assertTrue(context.contains("One edge of Type Post_hasCreator_Person, The post id 274877910399 has creator person id 166"));
        }

    }
}
