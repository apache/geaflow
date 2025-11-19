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

import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphAccessorFactory;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.IndexStoreCache;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.search.VectorSearcher;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SessionOperator {

    private final GraphAccessor graphAccessor = GraphAccessorFactory.INSTANCE.copy();

    public boolean vectorSearch(VectorSearch search) {
        //获取session
        SessionManagement manager = SessionManagement.INSTANCE;
        String sessionId = search.getSessionId();
        if (!manager.sessionExists(sessionId)) {
            return false;
        }
        //从管理器获取子图列表
        List<SubGraph> subGraphList = manager.getSubGraph(sessionId);
        //如果没有历史子图，进行全图匹配
        if (subGraphList == null || subGraphList.isEmpty()) {
            Iterator<GraphVertex> vertexIterator = graphAccessor.scanVertex();
            VectorSearcher searcher = new VectorSearcher(search);
            while (vertexIterator.hasNext()) {
                GraphVertex vertex = vertexIterator.next();
                //获取每个vertex的索引向量
                List<IVector> vertexIndex = IndexStoreCache.CACHE.getVertexIndex(vertex);
                searcher.add(vertex, vertexIndex);
            }
            //全局评分比对
            List<GraphVertex> startVertices = searcher.getResults().stream().filter(
                    v -> v instanceof GraphVertex
            ).map(v -> ((GraphVertex) v)).distinct().collect(Collectors.toList());
            List<SubGraph> subGraphs = startVertices.stream().map(v -> {
                SubGraph subGraph = new SubGraph();
                subGraph.addVertex(v);
                return subGraph;
            }).collect(Collectors.toList());
            manager.setSubGraph(sessionId, subGraphs);
            return true;
        }
        //遍历子图所有触点，进行向量搜索
        //全局评分比对
        //更新子图

        //如果检索完成，返回成功
        return true;
    }
}
