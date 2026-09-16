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

import java.util.*;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;

public class SessionOperator implements SearchOperator {

    private final GraphAccessor graphAccessor;
    private final IndexStore indexStore;

    /**
     * Graph scoped keyword index reused across queries. When {@code null} the operator falls back
     * to building a throw-away index per query, which is only kept for backward compatibility and
     * for equivalence testing.
     */
    private final ResidentSearchIndex residentIndex;

    public SessionOperator(GraphAccessor accessor, IndexStore store) {
        this(accessor, store, null);
    }

    public SessionOperator(GraphAccessor accessor, IndexStore store, ResidentSearchIndex residentIndex) {
        this.graphAccessor = Objects.requireNonNull(accessor);
        this.indexStore = Objects.requireNonNull(store);
        this.residentIndex = residentIndex;
    }

    @Override
    public List<SubGraph> apply(List<SubGraph> subGraphList, VectorSearch search) {
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
        String query = String.join(SearchConstants.DELIMITER, contents);
        if (subGraphList == null || subGraphList.isEmpty()) {
            List<GraphEntity> globalResults = searchWithGlobalGraph(query);
            List<GraphVertex> startVertices = new ArrayList<>();
            for (GraphEntity resEntity : globalResults) {
                if (resEntity instanceof GraphVertex) {
                    startVertices.add((GraphVertex) resEntity);
                }
            }
            //Apply to subgraph
            return startVertices.stream().map(v -> {
                SubGraph subGraph = new SubGraph();
                subGraph.addVertex(v);
                return subGraph;
            }).collect(Collectors.toList());
        } else {
            Map<GraphEntity, List<IVector>> extendEntityIndexMap = new HashMap<>();
            //Traverse all extension points of the subgraph and search within the extension area
            for (SubGraph subGraph : subGraphList) {
                List<GraphEntity> extendEntities = getSubgraphExpand(subGraph);
                for (GraphEntity extendEntity : extendEntities) {
                    List<IVector> entityIndex = indexStore.getEntityIndex(extendEntity);
                    extendEntityIndexMap.put(extendEntity, entityIndex);
                }
            }
            //recall compute, the candidate set is bounded by the subgraph expansion
            GraphSearchStore searchStore = initSearchStore(extendEntityIndexMap);
            searchStore.refresh();
            List<GraphEntity> matchEntities = searchStore.search(query, graphAccessor);
            closeQuietly(searchStore);
            Set<GraphEntity> matchEntitiesSet = new HashSet<>(matchEntities);

            //Apply to subgraph
            List<SubGraph> subGraphs = new ArrayList<>(subGraphList);
            for (SubGraph subGraph : subGraphs) {
                Set<GraphEntity> subgraphEntitySet = new HashSet<>(subGraph.getGraphEntityList());
                List<GraphEntity> extendEntities = getSubgraphExpand(subGraph);
                for (GraphEntity extendEntity : extendEntities) {
                    if (matchEntitiesSet.contains(extendEntity)
                            && !subgraphEntitySet.contains(extendEntity)) {
                        subgraphEntitySet.add(extendEntity);
                        subGraph.addEntity(extendEntity);
                    }
                }
            }
            return subGraphs;
        }
    }

    private List<GraphEntity> getSubgraphExpand(SubGraph subGraph) {
        List<GraphEntity> entityList = subGraph.getGraphEntityList();
        List<GraphEntity> expandEntities = new ArrayList<>();
        for (GraphEntity entity : entityList) {
            List<GraphEntity> entityExpand = graphAccessor.expand(entity);
            expandEntities.addAll(entityExpand);
        }
        return expandEntities;
    }

    private List<GraphEntity> searchWithGlobalGraph(String query) {
        if (residentIndex != null) {
            return residentIndex.searchWithIndex(graphAccessor, indexStore, query);
        }
        return searchWithGlobalGraphByRebuild(query);
    }

    /**
     * Legacy behaviour: scan the whole graph, build a throw-away index, search it, drop it.
     * Retained as the reference implementation for equivalence tests.
     */
    List<GraphEntity> searchWithGlobalGraphByRebuild(String query) {
        Map<GraphEntity, List<IVector>> entityIndexMap = new HashMap<>();
        Iterator<GraphVertex> vertexIterator = graphAccessor.scanVertex();
        while (vertexIterator.hasNext()) {
            GraphVertex vertex = vertexIterator.next();
            //Read all vertices indices from the index and add them to the candidate set.
            List<IVector> vertexIndex = indexStore.getEntityIndex(vertex);
            entityIndexMap.put(vertex, vertexIndex);
        }
        //recall compute
        GraphSearchStore searchStore = initSearchStore(entityIndexMap);
        searchStore.refresh();
        List<GraphEntity> result = searchStore.search(query, graphAccessor);
        closeQuietly(searchStore);
        return result;
    }

    private GraphSearchStore initSearchStore(Map<GraphEntity, List<IVector>> entityIndexMap) {
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
        return searchStore;
    }

    private void closeQuietly(GraphSearchStore searchStore) {
        try {
            searchStore.close();
        } catch (Throwable ignored) {
            // A throw-away store leaking is not worth failing the query for.
        }
    }
}
