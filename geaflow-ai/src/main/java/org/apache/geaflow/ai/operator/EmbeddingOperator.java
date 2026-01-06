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
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;

public class EmbeddingOperator implements SearchOperator {

    private final GraphAccessor graphAccessor;
    private final IndexStore indexStore;
    private double threshold;
    private int topN;

    public EmbeddingOperator(GraphAccessor accessor, IndexStore store) {
        this.graphAccessor = Objects.requireNonNull(accessor);
        this.indexStore = Objects.requireNonNull(store);
        this.threshold = Constants.EMBEDDING_OPERATE_DEFAULT_THRESHOLD;
        this.topN = Constants.EMBEDDING_OPERATE_DEFAULT_TOPN;
    }

    @Override
    public List<SubGraph> apply(List<SubGraph> subGraphList, VectorSearch search) {
        List<IVector> queryEmbeddingVectors = search.getVectorMap().get(VectorType.EmbeddingVector);
        if (queryEmbeddingVectors == null || queryEmbeddingVectors.isEmpty()) {
            if (subGraphList == null) {
                return new ArrayList<>();
            }
            return new ArrayList<>(subGraphList);
        }
        List<GraphEntity> globalResults = searchWithGlobalGraph(queryEmbeddingVectors);
        if (subGraphList == null || subGraphList.isEmpty()) {
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
            //recall compute
            List<GraphEntity> matchEntities = searchEmbeddings(queryEmbeddingVectors, extendEntityIndexMap);
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

    private List<GraphEntity> searchWithGlobalGraph(List<IVector> queryEmbeddingVectors) {
        Map<GraphEntity, List<IVector>> entityIndexMap = new HashMap<>();
        Iterator<GraphVertex> vertexIterator = graphAccessor.scanVertex();
        while (vertexIterator.hasNext()) {
            GraphVertex vertex = vertexIterator.next();
            //Read all vertices indices from the index and add them to the candidate set.
            List<IVector> vertexIndex = indexStore.getEntityIndex(vertex);
            entityIndexMap.put(vertex, vertexIndex);
        }
        //recall compute
        return searchEmbeddings(queryEmbeddingVectors, entityIndexMap);
    }

    private List<GraphEntity> searchEmbeddings(List<IVector> queryEmbeddingVectors,
                                               Map<GraphEntity, List<IVector>> entityIndexMap) {
        // Extract valid query EmbeddingVectors from input
        List<EmbeddingVector> queryVectors = queryEmbeddingVectors.stream()
                .filter(EmbeddingVector.class::isInstance)
                .map(EmbeddingVector.class::cast)
                .collect(Collectors.toList());

        // Create min-heap to maintain top N entities by maximum relevance score
        PriorityQueue<GraphEntityScorePair> minHeap = new PriorityQueue<>(Comparator.comparingDouble(a -> a.score));

        // Process each entity in the index
        for (Map.Entry<GraphEntity, List<IVector>> entry : entityIndexMap.entrySet()) {
            GraphEntity entity = entry.getKey();
            // Extract valid entity embeddings
            List<EmbeddingVector> entityVectors = entry.getValue().stream()
                    .filter(EmbeddingVector.class::isInstance)
                    .map(EmbeddingVector.class::cast)
                    .collect(Collectors.toList());

            // Skip entities without valid embeddings
            if (entityVectors.isEmpty()) {
                continue;
            }

            // Compute maximum relevance score between query and entity embeddings
            double maxRelevance = 0.0;
            for (EmbeddingVector queryVector : queryVectors) {
                for (EmbeddingVector entityVector : entityVectors) {
                    double matchScore = queryVector.match(entityVector);
                    if (matchScore > maxRelevance) {
                        maxRelevance = matchScore;
                    }
                }
            }

            // Add to candidates if above threshold
            if (maxRelevance > threshold) {
                // Maintain heap size <= topN
                if (minHeap.size() < topN) {
                    minHeap.offer(new GraphEntityScorePair(entity, maxRelevance));
                } else {
                    assert minHeap.peek() != null;
                    if (minHeap.peek().score < maxRelevance) {
                        minHeap.poll();
                        minHeap.offer(new GraphEntityScorePair(entity, maxRelevance));
                    }
                }
            }
        }

        // Convert heap to sorted list (descending by score)
        return minHeap.stream()
                .sorted(Comparator.comparingDouble((GraphEntityScorePair p) -> p.score).reversed())
                .map(pair -> pair.entity)
                .collect(Collectors.toList());
    }

    // Helper class to store entity-score pairs
    private static class GraphEntityScorePair {
        final GraphEntity entity;
        final double score;

        GraphEntityScorePair(GraphEntity entity, double score) {
            this.entity = entity;
            this.score = score;
        }
    }
}
