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
        //进行图检索
        String sessionId = search.getSessionId();
        String searchResult = server.search(search);
        Assert.assertNotNull(searchResult);
        //生成编码
        Context context = server.verbalize(sessionId,
                new SubgraphSemanticPromptFunction(graphAccessor));
        Assert.assertNotNull(context);
        return context.toString();
    }

    @Test
    public void testLdbcMainPipeline() {
        //加载图数据，并初始化图数据Accessor
        LocalFileGraphAccessor graphAccessor =
                new LocalFileGraphAccessor(this.getClass().getClassLoader(),
                        "org/apache/geaflow/ai/graph_ldbc_sf", 10000L);
        graphAccessor.getGraphSchema().setPromptFormatter(new LdbcPromptFormatter());
        GraphAccessorFactory.INSTANCE = graphAccessor;

        //创建图索引
        IndexStore indexStore = new EntityAttributeIndexStore();
        GraphMemoryServer server =  new GraphMemoryServer();
        //加载图数据，加载索引数据
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);

        //创建会话
        String sessionId = server.createSession();
        //创建图检索，选定检索算子
        VectorSearch search = new VectorSearch(null, sessionId);
        search.addVector(new KeywordVector("What comments has Chaim Azriel posted?"));
        System.out.println("Round 1: \n" + search);

        //进行图检索
        String context = produceCycle(server, search, graphAccessor);
        System.out.println("Round 1: \n" + context);
        Assert.assertTrue(context.contains("A Forum, name is Wall of Chaim Azriel Hleb, id is 220, created at 2010-02-19T12:42:24.255+00:00"));
        Assert.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is 166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
        Assert.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is 21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

        //创建图检索，选定检索算子
        search = new VectorSearch(null, sessionId);
        search.addVector(new KeywordVector("None of the comment IP addresses match either of the two known Chaim Azriel IPs.**\n" +
                "\n" +
                "Additionally, **no post or comment is explicitly linked to a user ID** in the data, and the **forum named \"Wall of Chaim Azriel Hleb\" (ID 220)** does not provide evidence of authored content unless we assume he posted on his own wall — but again, no such attribution is made.\n" +
                "\n" +
                "### Conclusion:\n" +
                "\n" +
                "\uD83D\uDD39 **There is insufficient information to confirm that any of the listed comments were posted by Chaim Azriel Hleb or Chaim Azriel Epstein.**\n" +
                "\n" +
                "The **IP addresses used in the comments do not match** those associated with either person, and **no user IDs are linked to the comments**.\n" +
                "\n" +
                "➡️ **Answer: Based on the provided data, we cannot determine any comments posted by Chaim Azriel. The information is insufficient."));
        System.out.println("Round 2: \n" + search);
        //进行图检索
        context = produceCycle(server, search, graphAccessor);
        System.out.println("Round 2: \n" + context);
        Assert.assertTrue(context.contains("A Forum, name is Wall of Chaim Azriel Hleb, id is 220, created at 2010-02-19T12:42:24.255+00:00"));
        Assert.assertTrue(context.contains("A Person, male, register at 2010-02-19T12:42:14.255+00:00, id is 166, name is Chaim Azriel Hleb, birthday is 1985-10-01, ip is 178.238.2.172, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com"));
        Assert.assertTrue(context.contains("A Person, male, register at 2011-09-18T02:40:31.062+00:00, id is 21990232556059, name is Chaim Azriel Epstein, birthday is 1981-07-17, ip is 80.94.167.126, use browser is Firefox, use language are ru;pl;en, email address is Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com"));

    }
}
