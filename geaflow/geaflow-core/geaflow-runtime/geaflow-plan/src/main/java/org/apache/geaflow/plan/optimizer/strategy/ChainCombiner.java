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

package org.apache.geaflow.plan.optimizer.strategy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.geaflow.operator.OpArgs.ChainStrategy;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.graph.VertexType;
import org.apache.geaflow.plan.util.DAGValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainCombiner implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChainCombiner.class);

    private Set<Integer> visited = new HashSet<>();

    public void combineVertex(PipelineGraph pipelineGraph) {
        visited.clear();

        // 1. Generate vertex id map according to vertex map.
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();

        // 2. Generate all output and input edges of all vertices according to edges.
        Map<Integer, Set<PipelineEdge>> outputEdges = new HashMap<>();
        Map<Integer, Set<PipelineEdge>> inputEdges = new HashMap<>();
        vertexMap.keySet().forEach(vertexId -> {
            outputEdges.put(vertexId, pipelineGraph.getVertexOutEdges(vertexId));
            inputEdges.put(vertexId, pipelineGraph.getVertexInputEdges(vertexId));
        });

        // 3. Find all the Source nodes and merge recursively from the Source node.
        List<PipelineVertex> sourceVertices = pipelineGraph.getPipelineVertices().stream()
            .filter(pipelineVertex -> (pipelineVertex.getType() == VertexType.source))
            .collect(Collectors.toList());

        Collection<PipelineEdge> jobEdges = pipelineGraph.getPipelineEdgeList();
        Collection<PipelineVertex> jobVertices = pipelineGraph.getPipelineVertices();

        List<Integer> verticesIds = jobVertices.stream().map(x -> x.getVertexId())
            .collect(Collectors.toList());
        List<PipelineEdge> needAddJobEdges = new ArrayList<>();
        for (PipelineEdge jobEdge : jobEdges) {
            if (!verticesIds.contains(jobEdge.getSrcId())) {
                sourceVertices.add(vertexMap.get(jobEdge.getTargetId()));
                needAddJobEdges.add(jobEdge);
            }
        }

        if (sourceVertices.size() != 0) {
            // 4. Recursively generates new nodes and edges.
            Set<PipelineVertex> newVertices = new HashSet<>();
            Set<PipelineEdge> newEdges = new TreeSet<>(new Comparator<PipelineEdge>() {
                @Override
                public int compare(PipelineEdge o1, PipelineEdge o2) {
                    return o1.getEdgeId() - o2.getEdgeId();
                }
            });
            for (PipelineVertex sourceVertex : sourceVertices) {
                newVertices.add(sourceVertex);
                createOperatorChain(sourceVertex.getVertexId(), sourceVertex, vertexMap, inputEdges,
                    outputEdges, newVertices, newEdges, null);
            }

            pipelineGraph.setPipelineEdges(newEdges);
            newVertices.forEach(jobVertex -> {
                LOGGER.info(jobVertex.getVertexString());
                DAGValidator.checkVertexValidity(pipelineGraph, jobVertex, false);
            });
            pipelineGraph.setPipelineVertices(newVertices);
            needAddJobEdges.stream().forEach(jobEdge -> pipelineGraph.addEdge(jobEdge));
        }
    }

    private void createOperatorChain(int id, PipelineVertex srcVertex, Map<Integer, PipelineVertex> vertexMap,
                                     Map<Integer, Set<PipelineEdge>> inputEdges,
                                     Map<Integer, Set<PipelineEdge>> outputEdges,
                                     Set<PipelineVertex> newVertices, Set<PipelineEdge> newEdges,
                                     String outputTag) {
        int srcId = srcVertex.getVertexId();

        if (visited.add(srcId)) {
            LOGGER.debug("Exploring vertex[{}]", srcId);
            if (outputEdges.containsKey(srcId)) {
                LOGGER.debug("srcId:{}", srcId);
                Set<PipelineEdge> srcVertexOutputEdges = outputEdges.get(srcId);
                for (PipelineEdge executeEdge : srcVertexOutputEdges) {
                    LOGGER.debug("edge:{}", executeEdge);
                    int targetId = executeEdge.getTargetId();
                    PipelineVertex targetVertex = vertexMap.get(targetId);
                    if (executeEdge.getEdgeName() != null) {
                        outputTag = executeEdge.getEdgeName();
                    }

                    if (isVertexCanMerge(srcVertex, targetVertex, executeEdge, inputEdges)) {
                        LOGGER.debug("Vertex[{}] can merge Vertex[{}]", srcVertex.getVertexId(),
                            targetVertex.getVertexId());

                        //Add a dependency for an Operator
                        AbstractOperator abstractOperator = (AbstractOperator) srcVertex.getOperator();
                        abstractOperator.addNextOperator(targetVertex.getOperator());
                        createOperatorChain(id, targetVertex, vertexMap, inputEdges, outputEdges,
                            newVertices, newEdges, outputTag);
                        srcVertex.setChainTailType(targetVertex.getChainTailType());
                        if (executeEdge.getEdgeName() != null) {
                            abstractOperator.getOutputTags().put(executeEdge.getEdgeId(), executeEdge.getEdgeName());
                        }
                    } else {
                        LOGGER.debug("Vertex[{}] can't merge Vertex[{}]", srcVertex.getVertexId(),
                            targetVertex.getVertexId());
                        executeEdge.setSrcId(id);
                        executeEdge.setEdgeName(outputTag);
                        newEdges.add(executeEdge);
                        newVertices.add(targetVertex);
                        if (executeEdge.getSrcId() != executeEdge.getTargetId()) {
                            createOperatorChain(targetVertex.getVertexId(), targetVertex, vertexMap,
                                inputEdges, outputEdges, newVertices, newEdges, null);
                        }
                    }
                }
            }
        }
    }

    /**
     * Determine whether single nodes can be merged.
     * Conditions for node merging:
     * 1. The entry degree of the target node is 1.
     * 2. The edge must be of type forward, not key.
     * 3. Concurrency must be consistent.
     * 4. The chainStrategy of the source node cannot be NEVER.
     * 5. The chainStrategy of the target node must be ALWAYS.
     */
    private boolean isVertexCanMerge(PipelineVertex srcVertex, PipelineVertex targetVertex,
                                     PipelineEdge executeEdge, Map<Integer, Set<PipelineEdge>> inputEdges) {

        if (inputEdges.get(targetVertex.getVertexId()).size() != 1) {
            return false;
        }

        if (executeEdge.getPartition().getPartitionType() != IPartitioner.PartitionType.forward) {
            return false;
        }

        if (srcVertex.getParallelism() != targetVertex.getParallelism()) {
            return false;
        }

        if (((AbstractOperator) srcVertex.getOperator()).getOpArgs().getChainStrategy()
            == ChainStrategy.NEVER) {
            return false;
        }

        ChainStrategy strategy = ((AbstractOperator) targetVertex.getOperator()).getOpArgs().getChainStrategy();
        if (strategy != null && strategy != ChainStrategy.ALWAYS) {
            return false;
        }

        return true;
    }
}
