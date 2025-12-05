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
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;

import java.util.*;
import java.util.stream.Collectors;

public class SessionOperator implements SearchOperator {

    private final GraphAccessor graphAccessor;
    private final IndexStore indexStore;

    public SessionOperator(GraphAccessor accessor, IndexStore store) {
        this.graphAccessor = Objects.requireNonNull(accessor);
        this.indexStore = Objects.requireNonNull(store);
    }

    @Override
    public List<SubGraph> apply(List<SubGraph> subGraphList, VectorSearch search) {
        //没有子图时进行全局匹配
        Map<GraphEntity, List<IVector>> entityIndexMap = new HashMap<>();
        List<IVector> keyWordVectors = search.getVectorMap().get(VectorType.KeywordVector);
        if (keyWordVectors == null || keyWordVectors.isEmpty()) {
            if (subGraphList == null) {
                return new ArrayList<>();
            }
            return new ArrayList<>(subGraphList);
        }
        List<String> contents = new ArrayList<>(keyWordVectors.size());
        for (IVector v : keyWordVectors) {
            contents.add(v.toString());
        }
        String query = String.join("  ", contents);
        if (subGraphList == null || subGraphList.isEmpty()) {
            //创建备选集
            Iterator<GraphVertex> vertexIterator = graphAccessor.scanVertex();
            while (vertexIterator.hasNext()) {
                GraphVertex vertex = vertexIterator.next();
                //从索引中读出所有点的索引，加入备选集
                List<IVector> vertexIndex = indexStore.getVertexIndex(vertex);
                entityIndexMap.put(vertex, vertexIndex);
            }
            //recall compute
            GraphSearchStore searchStore = new GraphSearchStore();
            for (Map.Entry<GraphEntity, List<IVector>> entry : entityIndexMap.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    if (entry.getKey() instanceof GraphVertex) {
                        searchStore.indexVertex((GraphVertex) entry.getKey(), entry.getValue());
                    } else if (entry.getKey() instanceof GraphEdge) {
                        searchStore.indexEdge((GraphEdge) entry.getKey(), entry.getValue());
                    }
                }
            }
            searchStore.close();
            List<GraphEntity> results = searchStore.search(query, graphAccessor);
            List<GraphVertex> startVertices = new ArrayList<>();
            for (GraphEntity resEntity : results) {
                if (resEntity instanceof GraphVertex) {
                    startVertices.add((GraphVertex) resEntity);
                }
            }
            //Apply to subgraph
            List<SubGraph> subGraphs = startVertices.stream().map(v -> {
                SubGraph subGraph = new SubGraph();
                subGraph.addVertex(v);
                return subGraph;
            }).collect(Collectors.toList());
            return subGraphs;
        } else {
            //遍历子图所有触点，进行搜索
            //创建备选集
            //子图触点加入备选集合
            //recall compute
            List<GraphEntity> matchEntities = new ArrayList<>();
            //Apply to subgraph
            List<SubGraph> subGraphs = new ArrayList<>(subGraphList);
            return subGraphs;
        }
    }
}
