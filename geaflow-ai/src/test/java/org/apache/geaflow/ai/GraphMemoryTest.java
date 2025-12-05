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
import org.apache.geaflow.ai.verbalization.PromptFunction;
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
        server.addGraphAccessor(new EmptyGraphAccessor());
        server.addIndexStore(indexStore);

        //创建会话
        String sessionId = server.createSession();
        //创建图检索
        VectorSearch search = new VectorSearch(null, sessionId);
        //进行图检索
        String context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt=''}");

        context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt=''}");
    }

    private String produceCycle(GraphMemoryServer server, VectorSearch search) {
        //进行图检索
        String sessionId = search.getSessionId();
        String searchResult = server.search(search);
        Assert.assertNotNull(searchResult);
        //生成编码
        Context context = server.verbalize(sessionId, new PromptFunction());
        Assert.assertNotNull(context);
        return context.toString();
    }

    @Test
    public void testLdbcMainPipeline() {
        //加载图数据，并初始化图数据Accessor
        LocalFileGraphAccessor graphAccessor =
                new LocalFileGraphAccessor(this.getClass().getClassLoader(),
                        "org/apache/geaflow/ai/graph_ldbc_sf", 10000L);
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

        //进行图检索
        String context = produceCycle(server, search);
        Assert.assertTrue(context.contains("SubGraph{graphEntityList=[Forum | 220 | 2010-02-19T12:42:24.255+00:00|220|Wall of Chaim Azriel Hleb]}"));
        Assert.assertTrue(context.contains("SubGraph{graphEntityList=[Person | 166 | 2010-02-19T12:42:14.255+00:00|166|Chaim Azriel|Hleb|male|1985-10-01|178.238.2.172|Firefox|ru;pl;en|Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com]}"));
        Assert.assertTrue(context.contains("SubGraph{graphEntityList=[Person | 21990232556059 | 2011-09-18T02:40:31.062+00:00|21990232556059|Chaim Azriel|Epstein|male|1981-07-17|80.94.167.126|Firefox|ru;pl;en|Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com]}"));

    }
}
