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

import org.apache.geaflow.ai.graph.GraphAccessorFactory;
import org.apache.geaflow.ai.graph.LocalFileGraphAccessor;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.index.vector.MagnitudeVector;
import org.apache.geaflow.ai.index.vector.TraversalVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.reduce.ReduceFunction;
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
        String searchResult = server.vectorSearch(search);
        Assert.assertNotNull(searchResult);
        //缩减图结构
        String reduceResult = server.reduce(sessionId, new ReduceFunction());
        Assert.assertNotNull(reduceResult);
        //生成编码
        Context context = server.verbalize(sessionId, new PromptFunction());
        Assert.assertNotNull(context);
        return context.toString();
    }

    @Test
    public void testLdbcMainPipeline() {
        LocalFileGraphAccessor graphAccessor =
                new LocalFileGraphAccessor(this.getClass().getClassLoader(),
                        "org/apache/geaflow/ai/graph_ldbc_sf", 10000L);
        GraphAccessorFactory.INSTANCE = graphAccessor;

        GraphMemoryServer server =  new GraphMemoryServer();
        //创建会话
        String sessionId = server.createSession();
        //创建图检索
        VectorSearch search = new VectorSearch(null, sessionId);
        search.addVector(new KeywordVector("What comments has Chaim Azriel posted?"));
        search.addVector(new TraversalVector(
                "Comment", "Comment_hasCreator_Person", "Person",
                "Person", "Comment_hasCreator_Person", "Comment"));
        //进行图检索
        String context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt='SubGraph{graphEntityList=[Forum | 220 | 2010-02-19T12:42:24.255+00:00|220|Wall of Chaim Azriel Hleb|220|Forum]}\n" +
                "SubGraph{graphEntityList=[Comment | 1030792159190 | 2012-08-17T00:07:59.936+00:00|1030792159190|203.144.56.205|Internet Explorer|About Dusty Springfield, s What Have I Done to Deserve This?, Nothing Has BeenAbout Pure Morning, g (199|104|1030792159190|Comment]}\n" +
                "SubGraph{graphEntityList=[Post | 687194791775 | 2011-10-12T05:11:22.087+00:00|687194791775||46.249.37.192|Firefox|nl|About Mariano Rivera,  has posted an ERA under 2.About Freddie Mercury,  1991, only one day after pAbout|104|687194791775|Post]}\n" +
                "SubGraph{graphEntityList=[Person | 166 | 2010-02-19T12:42:14.255+00:00|166|Chaim Azriel|Hleb|male|1985-10-01|178.238.2.172|Firefox|ru;pl;en|Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com|166|Person]}\n" +
                "SubGraph{graphEntityList=[Person | 21990232556059 | 2011-09-18T02:40:31.062+00:00|21990232556059|Chaim Azriel|Epstein|male|1981-07-17|80.94.167.126|Firefox|ru;pl;en|Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com|21990232556059|Person]}\n" +
                "'}");

        context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt='SubGraph{graphEntityList=[Forum | 220 | 2010-02-19T12:42:24.255+00:00|220|Wall of Chaim Azriel Hleb|220|Forum]}\n" +
                "SubGraph{graphEntityList=[Comment | 1030792159190 | 2012-08-17T00:07:59.936+00:00|1030792159190|203.144.56.205|Internet Explorer|About Dusty Springfield, s What Have I Done to Deserve This?, Nothing Has BeenAbout Pure Morning, g (199|104|1030792159190|Comment]}\n" +
                "SubGraph{graphEntityList=[Post | 687194791775 | 2011-10-12T05:11:22.087+00:00|687194791775||46.249.37.192|Firefox|nl|About Mariano Rivera,  has posted an ERA under 2.About Freddie Mercury,  1991, only one day after pAbout|104|687194791775|Post]}\n" +
                "SubGraph{graphEntityList=[Person | 166 | 2010-02-19T12:42:14.255+00:00|166|Chaim Azriel|Hleb|male|1985-10-01|178.238.2.172|Firefox|ru;pl;en|Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com|166|Person]}\n" +
                "SubGraph{graphEntityList=[Person | 21990232556059 | 2011-09-18T02:40:31.062+00:00|21990232556059|Chaim Azriel|Epstein|male|1981-07-17|80.94.167.126|Firefox|ru;pl;en|Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com|21990232556059|Person]}\n" +
                "'}");

        context = produceCycle(server, search);
        Assert.assertEquals(context, "Context{prompt='SubGraph{graphEntityList=[Forum | 220 | 2010-02-19T12:42:24.255+00:00|220|Wall of Chaim Azriel Hleb|220|Forum]}\n" +
                "SubGraph{graphEntityList=[Comment | 1030792159190 | 2012-08-17T00:07:59.936+00:00|1030792159190|203.144.56.205|Internet Explorer|About Dusty Springfield, s What Have I Done to Deserve This?, Nothing Has BeenAbout Pure Morning, g (199|104|1030792159190|Comment]}\n" +
                "SubGraph{graphEntityList=[Post | 687194791775 | 2011-10-12T05:11:22.087+00:00|687194791775||46.249.37.192|Firefox|nl|About Mariano Rivera,  has posted an ERA under 2.About Freddie Mercury,  1991, only one day after pAbout|104|687194791775|Post]}\n" +
                "SubGraph{graphEntityList=[Person | 166 | 2010-02-19T12:42:14.255+00:00|166|Chaim Azriel|Hleb|male|1985-10-01|178.238.2.172|Firefox|ru;pl;en|Chaim.Azriel166@yahoo.com;Chaim.Azriel166@gmail.com;Chaim.Azriel166@gmx.com;Chaim.Azriel166@hotmail.com;Chaim.Azriel166@theblackmarket.com|166|Person]}\n" +
                "SubGraph{graphEntityList=[Person | 21990232556059 | 2011-09-18T02:40:31.062+00:00|21990232556059|Chaim Azriel|Epstein|male|1981-07-17|80.94.167.126|Firefox|ru;pl;en|Chaim.Azriel21990232556059@gmx.com;Chaim.Azriel21990232556059@gmail.com;Chaim.Azriel21990232556059@hotmail.com;Chaim.Azriel21990232556059@yahoo.com;Chaim.Azriel21990232556059@zoho.com|21990232556059|Person]}\n" +
                "'}");
    }
}
