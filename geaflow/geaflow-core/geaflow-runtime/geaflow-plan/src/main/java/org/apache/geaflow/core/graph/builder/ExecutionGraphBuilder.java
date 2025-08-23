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

package org.apache.geaflow.core.graph.builder;

import static org.apache.geaflow.plan.PipelinePlanBuilder.ITERATION_AGG_VERTEX_ID;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.core.graph.CollectExecutionVertex;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionEdge;
import org.apache.geaflow.core.graph.ExecutionGraph;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.core.graph.ExecutionVertexGroupEdge;
import org.apache.geaflow.core.graph.IteratorExecutionVertex;
import org.apache.geaflow.core.graph.plan.visualization.ExecutionGraphVisualization;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricAggOp;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.AbstractDynamicGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.graph.compute.statical.AbstractStaticGraphVertexCentricOp;
import org.apache.geaflow.plan.graph.AffinityLevel;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.graph.VertexType;
import org.apache.geaflow.processor.Processor;
import org.apache.geaflow.processor.builder.IProcessorBuilder;
import org.apache.geaflow.processor.builder.ProcessorBuilder;
import org.apache.geaflow.processor.impl.AbstractProcessor;
import org.apache.geaflow.processor.impl.window.TwoInputProcessor;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.utils.math.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for building execution graph.
 */
public class ExecutionGraphBuilder implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraphBuilder.class);

    private static final int START_GROUP_ID = 1;

    private PipelineGraph plan;
    private int flyingCount;

    public ExecutionGraphBuilder(PipelineGraph plan) {
        this.plan = plan;
    }

    public ExecutionGraph buildExecutionGraph(Configuration jobConf) {
        ExecutionGraph executionGraph = new ExecutionGraph();

        Map<Integer, ExecutionVertex> id2Vertexes = new HashMap<>();

        // Build execution vertex group.
        List<PipelineVertex> pipelineVertexList = plan.getSourceVertices();
        Queue<PipelineVertex> pipelineVertexQueue = new LinkedList<>(pipelineVertexList);
        Map<Integer, Integer> vertexId2GroupIdMap = new HashMap<>();

        Map<Integer, ExecutionVertexGroup> vertexGroupMap = buildExecutionVertexGroup(
            vertexId2GroupIdMap, pipelineVertexQueue);
        executionGraph.setVertexGroupMap(vertexGroupMap);

        vertexGroupMap.values().stream().forEach(vertexGroup -> vertexGroup.getVertexMap().values().stream()
            .forEach(vertex -> id2Vertexes.put(vertex.getVertexId(), vertex)));

        // Rebuild the input and output vertex group for the group, and execution vertexes for the vertex at the same time.
        Map<Integer, ExecutionVertexGroupEdge> groupEdgeMap = new HashMap<>();
        for (Map.Entry<Integer, ExecutionVertexGroup> vertexGroupEntry : vertexGroupMap.entrySet()) {
            ExecutionVertexGroup vertexGroup = vertexGroupEntry.getValue();
            // 1. Build the input and output execution vertexes for the vertex.
            for (Map.Entry<Integer, ExecutionVertex> entry : vertexGroupEntry.getValue().getVertexMap().entrySet()) {
                int vertexId = entry.getKey();
                Set<PipelineEdge> vertexOutEdges = plan.getVertexOutEdges(vertexId);
                Map<Integer, ExecutionEdge> outEdgeMap = new HashMap<>();
                Map<Integer, ExecutionEdge> inEdgeMap = new HashMap<>();

                for (PipelineEdge pipelineEdge : vertexOutEdges) {
                    ExecutionEdge executionEdge = buildEdge(pipelineEdge);
                    outEdgeMap.put(pipelineEdge.getEdgeId(), executionEdge);
                }
                vertexGroup.getEdgeMap().putAll(outEdgeMap);
                vertexGroup.putVertexId2OutEdgeIds(vertexId, plan.getVertexOutEdges(vertexId)
                    .stream().map(PipelineEdge::getEdgeId).collect(Collectors.toList()));

                Set<PipelineEdge> vertexInEdges = plan.getVertexInputEdges(vertexId);
                for (PipelineEdge pipelineEdge : vertexInEdges) {
                    ExecutionEdge executionEdge = buildEdge(pipelineEdge);
                    inEdgeMap.put(pipelineEdge.getEdgeId(), executionEdge);
                }
                vertexGroup.getEdgeMap().putAll(inEdgeMap);
                vertexGroup.putVertexId2InEdgeIds(vertexId, plan.getVertexInputEdges(vertexId)
                    .stream().map(PipelineEdge::getEdgeId).collect(Collectors.toList()));

                entry.getValue().setInputEdges(inEdgeMap.values().stream().collect(Collectors.toList()));
                entry.getValue().setOutputEdges(outEdgeMap.values().stream().collect(Collectors.toList()));
            }

            // 2. Build the input and output vertex group for the group.
            int groupId = vertexGroupEntry.getKey();
            List<Integer> outGroupEdgeIds = new ArrayList<>();
            List<Integer> inGroupEdgeIds = new ArrayList<>();

            List<Integer> tailVertexIds = vertexGroup.getTailVertexIds();
            List<Integer> headVertexIds = vertexGroup.getHeadVertexIds();

            headVertexIds.stream().forEach(headVertexId -> {
                inGroupEdgeIds.addAll(plan.getVertexInputEdges(headVertexId).stream()
                    // Exclude self-loop edge.
                    .filter(e -> !vertexGroup.getVertexMap().containsKey(e.getSrcId()))
                    .map(PipelineEdge::getEdgeId).collect(Collectors.toList()));
                vertexGroup.getParentVertexGroupIds().addAll(plan.getVertexInputVertexIds(headVertexId).stream()
                    .filter(vertexId -> !vertexGroup.getVertexMap().containsKey(vertexId))
                    .map(vertexId -> vertexId2GroupIdMap.get(vertexId)).collect(Collectors.toList()));
            });

            tailVertexIds.stream().forEach(tailVertexId -> {
                outGroupEdgeIds.addAll(plan.getVertexOutEdges(tailVertexId).stream()
                    .filter(e -> vertexGroup.getVertexMap().containsKey(e.getTargetId()))
                    .map(PipelineEdge::getEdgeId).collect(Collectors.toList()));
                vertexGroup.getChildrenVertexGroupIds().addAll(plan.getVertexOutputVertexIds(tailVertexId).stream()
                    .filter(vertexId -> !vertexGroup.getVertexMap().containsKey(vertexId))
                    .map(vertexId -> vertexId2GroupIdMap.get(vertexId)).collect(Collectors.toList()));
            });

            executionGraph.putVertexGroupInEdgeIds(groupId, inGroupEdgeIds);
            executionGraph.putVertexGroupOutEdgeIds(groupId, outGroupEdgeIds);

            for (int tailVertexId : tailVertexIds) {
                Set<PipelineEdge> pipelineEdgeSet = plan.getVertexOutEdges(tailVertexId);
                pipelineEdgeSet.stream().forEach(pipelineEdge -> groupEdgeMap.put(pipelineEdge.getEdgeId(), new ExecutionVertexGroupEdge(
                    pipelineEdge.getPartition(), pipelineEdge.getEdgeId(), pipelineEdge.getEdgeName(),
                    vertexId2GroupIdMap.get(tailVertexId), vertexId2GroupIdMap.get(pipelineEdge.getTargetId()))));
            }
        }
        executionGraph.setGroupEdgeMap(groupEdgeMap);

        flyingCount = jobConf.getInteger(FrameworkConfigKeys.STREAMING_FLYING_BATCH_NUM);
        buildCycleGroupMeta(executionGraph);

        ExecutionGraphVisualization graphVisualization = new ExecutionGraphVisualization(executionGraph);
        LOGGER.info("execution graph: {}, \nvertex group size: {}, group edge size: {}",
            graphVisualization.getExecutionGraphViz(), executionGraph.getVertexGroupMap().size(), executionGraph.getGroupEdgeMap().size());

        return executionGraph;
    }

    /**
     * Build execution vertex group.
     */
    private Map<Integer, ExecutionVertexGroup> buildExecutionVertexGroup(Map<Integer, Integer> vertexId2GroupIdMap,
                                                                         Queue<PipelineVertex> pipelineVertexQueue) {
        Map<Integer, ExecutionVertexGroup> vertexGroupMap = new HashMap<>();
        int groupId = START_GROUP_ID;

        Set<Integer> groupedVertices = new HashSet<>();

        while (!pipelineVertexQueue.isEmpty()) {
            PipelineVertex pipelineVertex = pipelineVertexQueue.poll();
            // Ignore already grouped vertex.
            if (groupedVertices.contains(pipelineVertex.getVertexId())) {
                continue;
            }
            Map<Integer, ExecutionVertex> currentVertexGroupMap =
                group(pipelineVertex, pipelineVertexQueue, groupedVertices);
            ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(groupId);
            vertexGroupMap.put(groupId, vertexGroup);
            vertexGroup.getVertexMap().putAll(currentVertexGroupMap);
            for (int id : currentVertexGroupMap.keySet()) {
                vertexId2GroupIdMap.put(id, groupId);
            }
            groupedVertices.addAll(currentVertexGroupMap.keySet());
            groupId++;
        }
        return vertexGroupMap;
    }

    /**
     * Build execution vertices map for current vertex.
     *
     * @param vertex                Build group vertex map from the vertex.
     * @param triggerVertices       Vertices that can trigger to build or join an execution group.
     * @param globalGroupedVertices Already grouped vertices.
     * @return Vertex map that can join together to build an execution group.
     */
    private Map<Integer, ExecutionVertex> group(PipelineVertex vertex,
                                                Queue<PipelineVertex> triggerVertices,
                                                final Set<Integer> globalGroupedVertices) {
        Map<Integer, ExecutionVertex> currentVertexGroupMap = new HashMap<>();
        Queue<PipelineVertex> currentOutput = new LinkedList<>();
        Set<Integer> currentVisited = new HashSet<>();
        // If current vertex cannot group, build a vertex group that only include current vertex.
        if (!group(vertex, currentVertexGroupMap, currentVisited, currentOutput, globalGroupedVertices)) {

            currentVertexGroupMap.put(vertex.getVertexId(),
                buildExecutionVertex(plan.getVertexMap().get(vertex.getVertexId())));
            // Add output vertex to trigger next group.
            List<Integer> outputVertexIds = plan.getVertexOutputVertexIds(vertex.getVertexId());
            for (int id : outputVertexIds) {
                if (isReadyToGroup(id, globalGroupedVertices, currentVertexGroupMap.keySet())) {
                    PipelineVertex outputVertex = plan.getVertexMap().get(id);
                    triggerVertices.add(outputVertex);
                }
            }
        } else {
            // Current group is standalone pipeline which has no output vertices, can join into next group.
            while (currentOutput.isEmpty() && !triggerVertices.isEmpty()) {
                PipelineVertex nextVertex = triggerVertices.poll();
                if (!group(nextVertex, currentVertexGroupMap, currentVisited, currentOutput, globalGroupedVertices)) {
                    break;
                }
            }
            triggerVertices.addAll(currentOutput);
        }
        return currentVertexGroupMap;
    }

    /**
     * Check and build group for input vertex.
     */
    private boolean group(PipelineVertex vertex,
                          Map<Integer, ExecutionVertex> currentVertexGroupMap,
                          Set<Integer> currentVisited,
                          Queue<PipelineVertex> groupOutputVertices,
                          final Set<Integer> globalGroupedVertices) {

        currentVisited.add(vertex.getVertexId());
        if (!canGroup(vertex)) {
            return false;
        }
        // 1. All input must support group into current vertex.
        boolean canGroup = pushUpGroup(vertex, currentVertexGroupMap, currentVisited,
            groupOutputVertices, globalGroupedVertices);
        if (canGroup) {
            currentVertexGroupMap.put(vertex.getVertexId(),
                buildExecutionVertex(plan.getVertexMap().get(vertex.getVertexId())));
        }

        // 2. Try check and join output vertex into current group.
        if (canGroup) {
            pushDownGroup(vertex, currentVertexGroupMap, currentVisited,
                groupOutputVertices, globalGroupedVertices);
        }

        return canGroup;
    }


    /**
     * Check upstream vertex whether can group together, if could then group together.
     */
    private boolean pushUpGroup(PipelineVertex vertex,
                                Map<Integer, ExecutionVertex> currentVertexGroupMap,
                                Set<Integer> currentVisited,
                                Queue<PipelineVertex> groupOutputVertices,
                                final Set<Integer> globalGroupedVertices) {

        // The current vertex can group only if all input can group.
        // 1. All input must support group into current vertex.
        List<Integer> inputVertexIds = plan.getVertexInputVertexIds(vertex.getVertexId());
        List<Integer> inputVertexIdCandidates = new ArrayList<>();
        for (int id : inputVertexIds) {
            PipelineVertex inputVertex = plan.getVertexMap().get(id);
            // Input already in current vertex group, ignore.
            if (currentVertexGroupMap.containsKey(id) || globalGroupedVertices.contains(id)) {
                continue;
            } else if (id == vertex.getVertexId()) {
                // Ignore self loop edge.
                continue;
            } else if (currentVisited.contains(id)) {
                // Visited but not in grouped vertices, it means group failed in previous steps.
                // return false;
                continue;
            }
            if (!canGroup(inputVertex, vertex)) {
                return false;
            }
            inputVertexIdCandidates.add(id);
        }

        // 2. Try group input.
        Map<Integer, ExecutionVertex> inputVertices = new HashMap<>();
        for (int id : inputVertexIdCandidates) {
            PipelineVertex inputVertex = plan.getVertexMap().get(id);
            // Input already in current vertex group, ignore.
            if (currentVertexGroupMap.containsKey(id) || globalGroupedVertices.contains(id) || inputVertices.containsKey(id)) {
                continue;
            } else if (currentVisited.contains(id)) {
                // Visited but not in grouped vertices, it means group failed in previous steps.
                return false;
            }
            if (!canGroup(inputVertex, vertex)) {
                return false;
            }

            Map<Integer, ExecutionVertex> inputVertexGroupMap = new HashMap<>();
            inputVertexGroupMap.putAll(currentVertexGroupMap);
            boolean canGroup = group(inputVertex,
                inputVertexGroupMap, currentVisited,
                groupOutputVertices, globalGroupedVertices);
            if (!canGroup) {
                return false;
            } else {
                inputVertices.putAll(inputVertexGroupMap);
            }
        }
        // 3. All inputs can group into current group.
        currentVertexGroupMap.putAll(inputVertices);
        return true;
    }

    /**
     * Try check and join output vertices into current vertex group.
     * If one output can join into current vertex group, recursively group output vertex,
     * otherwise, add the output into queue to trigger next group.
     */
    private void pushDownGroup(PipelineVertex vertex,
                               Map<Integer, ExecutionVertex> currentVertexGroupMap,
                               Set<Integer> currentVisited,
                               Queue<PipelineVertex> groupOutputVertices,
                               final Set<Integer> globalGroupedVertices) {
        List<Integer> outputVertexIds = plan.getVertexOutputVertexIds(vertex.getVertexId());
        for (int id : outputVertexIds) {
            PipelineVertex outputVertex = plan.getVertexMap().get(id);

            // Ignore visited vertex.
            if (currentVertexGroupMap.containsKey(id)
                || globalGroupedVertices.contains(id)) {
                continue;
            }

            // If it cannot group, add to trigger queue.
            if (!canGroup(vertex, outputVertex)
                || !group(outputVertex, currentVertexGroupMap,
                currentVisited, groupOutputVertices, globalGroupedVertices)) {
                groupOutputVertices.add(outputVertex);
            }
        }
    }

    /**
     * Check the current pipeline vertex whether can group.
     */
    private boolean canGroup(PipelineVertex currentVertex) {
        boolean enGroup = true;

        VertexType type = currentVertex.getType();
        switch (type) {
            case vertex_centric:
            case inc_vertex_centric:
            case iterator:
            case inc_iterator:
            case iteration_aggregation:
                if (currentVertex.getOperator() instanceof IGraphVertexCentricAggOp) {
                    return true;
                }
                return false;
            default:
                AbstractOperator operator = (AbstractOperator) currentVertex.getOperator();
                if (!operator.getOpArgs().isEnGroup()) {
                    enGroup = false;
                } else {
                    List<Operator> operatorList = operator.getNextOperators();
                    for (Operator op : operatorList) {
                        if (!((AbstractOperator) op).getOpArgs().isEnGroup()) {
                            enGroup = false;
                            break;
                        }
                    }
                }
        }
        return enGroup;
    }

    /**
     * Check the current pipeline vertex can group with output vertex.
     */
    private boolean canGroup(PipelineVertex currentVertex, PipelineVertex outputVertex) {
        if (canGroupWithInput(currentVertex, outputVertex) && canGroupWithOutput(outputVertex, currentVertex)) {
            return true;
        }
        return false;
    }

    /**
     * Check the current pipeline vertex can group with output vertex.
     */
    private boolean canGroupWithOutput(PipelineVertex currentVertex, PipelineVertex outputVertex) {
        VertexType type = currentVertex.getType();
        switch (type) {
            case vertex_centric:
            case inc_vertex_centric:
            case iterator:
            case inc_iterator:
            case iteration_aggregation:
                if (currentVertex.getOperator() instanceof IGraphVertexCentricAggOp
                    && outputVertex.getOperator() instanceof IGraphVertexCentricAggOp) {
                    return true;
                }
                return false;
            default:
                return true;
        }
    }

    /**
     * Check the current pipeline vertex can group with input vertex.
     */
    private boolean canGroupWithInput(PipelineVertex currentVertex, PipelineVertex inputVertex) {
        VertexType type = currentVertex.getType();
        switch (type) {
            case vertex_centric:
            case inc_vertex_centric:
            case iterator:
            case inc_iterator:
            case iteration_aggregation:
                if (currentVertex.getOperator() instanceof IGraphVertexCentricAggOp
                    && inputVertex.getOperator() instanceof IGraphVertexCentricAggOp) {
                    return true;
                }
                return false;
            default:
                return true;
        }
    }

    /**
     * Check whether current vertex can start to build a new group.
     */
    private boolean isReadyToGroup(int vertexId,
                                   Set<Integer> globalGroupedVertices,
                                   Set<Integer> currentGroupedVertices) {

        List<Integer> inputs = plan.getVertexInputVertexIds(vertexId);
        for (int inputVid : inputs) {
            // A vertex that matches flowing cases will not allow to start a new group:
            //   1. input has not grouped.
            //   2. and input vertex is itself.
            //   3. and input is agg vertex that will never allow to group standalone.
            if (!globalGroupedVertices.contains(inputVid) && !currentGroupedVertices.contains(inputVid)
                && inputVid != vertexId && inputVid != ITERATION_AGG_VERTEX_ID) {
                return false;
            }
        }
        return true;
    }

    /**
     * Build execution vertex.
     */
    private ExecutionVertex buildExecutionVertex(PipelineVertex pipelineVertex) {
        ExecutionVertex executionVertex;

        int vertexId = pipelineVertex.getVertexId();
        VertexType type = pipelineVertex.getType();
        String name = pipelineVertex.getName();
        LOGGER.info("vertexId:{} vertexName:{} type:{}", vertexId, name, type);

        switch (type) {
            case vertex_centric:
            case inc_vertex_centric:
            case iterator:
            case inc_iterator:
                executionVertex = new IteratorExecutionVertex(vertexId, name, pipelineVertex.getIterations());
                break;
            case collect:
                executionVertex = new CollectExecutionVertex(vertexId, name);
                break;
            case inc_process:
            default:
                executionVertex = new ExecutionVertex(vertexId, pipelineVertex.getName());
                break;
        }

        // Construct the parent and child stage ids for the current stage.
        List<Integer> parentVertexIds = plan.getVertexInputVertexIds(executionVertex.getVertexId());
        if (parentVertexIds != null) {
            executionVertex.setParentVertexIds(parentVertexIds);
        }
        List<Integer> childrenStageIds = plan.getVertexOutputVertexIds(executionVertex.getVertexId());
        boolean hasChildren = childrenStageIds != null && childrenStageIds.size() > 0;
        // Build the downstream partition num for the current vertex.
        if (hasChildren) {
            int bucketNum = 1;
            Map<Integer, PipelineVertex> vertexMap = plan.getVertexMap();
            for (Integer childVertexId : childrenStageIds) {
                int childParallelism = getMaxParallelism(vertexMap.get(childVertexId));
                if (childParallelism > bucketNum) {
                    bucketNum = childParallelism;
                }
            }
            executionVertex.setNumPartitions(bucketNum);
        } else {
            executionVertex.setNumPartitions(pipelineVertex.getParallelism());
        }

        // Build execution processor.
        Processor processor = null;
        if (pipelineVertex.getOperator() != null) {
            IProcessorBuilder processorBuilder = new ProcessorBuilder();
            processor = processorBuilder.buildProcessor(pipelineVertex.getOperator());
            executionVertex.setProcessor(processor);
        }

        // Set join and combine left/right input processor stream name.
        if (pipelineVertex.getType() == VertexType.join || pipelineVertex.getType() == VertexType.combine) {
            List<PipelineEdge> edges = plan.getVertexInputEdges(pipelineVertex.getVertexId())
                .stream().sorted(Comparator.comparingInt(PipelineEdge::getStreamOrdinal))
                .collect(Collectors.toList());
            TwoInputProcessor twoInputProcessor = (TwoInputProcessor) processor;
            twoInputProcessor.setLeftStream(edges.get(0).getEdgeName());
            twoInputProcessor.setRightStream(edges.get(1).getEdgeName());
        }

        // Set other member param.
        executionVertex.setParallelism(pipelineVertex.getParallelism());
        executionVertex.setMaxParallelism(getMaxParallelism(pipelineVertex));
        executionVertex.setVertexType(pipelineVertex.getType());
        executionVertex.setAffinityLevel(pipelineVertex.getAffinity());
        executionVertex.setChainTailType(pipelineVertex.getChainTailType());

        LOGGER.info("execution vertex {}, parallelism {}, max parallelism {}, num partitions {}",
            executionVertex, executionVertex.getParallelism(), executionVertex.getMaxParallelism(), executionVertex.getNumPartitions());
        return executionVertex;
    }

    /**
     * Build the cycle group meta for graph.
     */
    private void buildCycleGroupMeta(ExecutionGraph graph) {
        Map<Integer, ExecutionVertexGroup> vertexGroupMap = graph.getVertexGroupMap();

        Set<ExecutionVertexGroup> pipelineSet = new HashSet<>();
        Set<ExecutionVertexGroup> batchSet = new HashSet<>();
        Set<ExecutionVertexGroup> iteratorSet = new HashSet<>();

        for (ExecutionVertexGroup vertexGroup : vertexGroupMap.values()) {
            for (ExecutionVertex vertex : vertexGroup.getVertexMap().values()) {
                LOGGER.info("vertexInfo:{}", vertex);
                CycleGroupType type = getCycleGroupType(vertex, ((AbstractProcessor) vertex.getProcessor()).getOperator());
                switch (type) {
                    case pipelined:
                        pipelineSet.add(vertexGroup);
                        vertexGroup.getCycleGroupMeta().setIterationCount(Long.MAX_VALUE);
                        vertexGroup.getCycleGroupMeta().setFlyingCount(flyingCount);
                        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
                        break;
                    case incremental:
                        if (vertex.getVertexType() == VertexType.iteration_aggregation) {
                            continue;
                        }
                        graph.getCycleGroupMeta().setIterationCount(Long.MAX_VALUE);
                        iteratorSet.add(vertexGroup);
                        vertexGroup.getCycleGroupMeta().setIterationCount(((IteratorExecutionVertex) vertex).getIteratorCount());
                        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
                        vertexGroup.getCycleGroupMeta().setAffinityLevel(AffinityLevel.worker);
                        break;
                    case statical:
                        if (vertex.getVertexType() == VertexType.iteration_aggregation) {
                            continue;
                        }
                        List<PipelineVertex> sourceVertexList = plan.getSourceVertices();
                        boolean isSingleWindow = sourceVertexList.stream().allMatch(v ->
                            ((AbstractOperator) v.getOperator()).getOpArgs().getOpType() == OpArgs.OpType.SINGLE_WINDOW_SOURCE);
                        if (!isSingleWindow) {
                            graph.getCycleGroupMeta().setIterationCount(Long.MAX_VALUE);
                        }
                        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.statical);
                        iteratorSet.add(vertexGroup);
                        vertexGroup.getCycleGroupMeta().setIterationCount(((IteratorExecutionVertex) vertex).getIteratorCount());
                        vertexGroup.getCycleGroupMeta().setAffinityLevel(AffinityLevel.worker);
                        break;
                    case windowed:
                        batchSet.add(vertexGroup);
                        vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.windowed);
                        break;
                    default:
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.operatorTypeNotSupportError(String.valueOf(type)));
                }
            }
        }

        // Currently not support pipeline mode in hybrid node cycle level, we will support in later.
        if (batchSet.size() > 0 || iteratorSet.size() > 0) {
            pipelineSet.stream().forEach(executionVertexGroup -> {
                executionVertexGroup.getCycleGroupMeta().setIterationCount(1);
                executionVertexGroup.getCycleGroupMeta().setFlyingCount(1);
            });
            // Build graph cycle group type.
            if (batchSet.size() > 0) {
                graph.getCycleGroupMeta().setGroupType(CycleGroupType.windowed);
            }
            if (iteratorSet.size() > 0) {
                if (iteratorSet.stream().anyMatch(executionVertexGroup
                    -> executionVertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.incremental)) {
                    graph.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
                } else {
                    graph.getCycleGroupMeta().setGroupType(CycleGroupType.statical);
                }
            }
        }

        // Set the affinity level for pipeline and batch vertex group.
        pipelineSet.stream().forEach(executionVertexGroup ->
            executionVertexGroup.getCycleGroupMeta().setAffinityLevel(buildGroupAffinityLevel(executionVertexGroup)));
        batchSet.stream().forEach(executionVertexGroup ->
            executionVertexGroup.getCycleGroupMeta().setAffinityLevel(buildGroupAffinityLevel(executionVertexGroup)));

        pipelineSet.clear();
        batchSet.clear();
        iteratorSet.clear();
    }

    /**
     * Get the cycle group type for current op.
     */
    private CycleGroupType getCycleGroupType(ExecutionVertex vertex, Operator operator) {
        CycleGroupType groupType;
        OpArgs.OpType type = ((AbstractOperator) operator).getOpArgs().getOpType();
        switch (type) {
            case ONE_INPUT:
            case TWO_INPUT:
            case MULTI_WINDOW_SOURCE:
            case GRAPH_SOURCE:
                groupType = CycleGroupType.pipelined;
                // Get type of sub operator.
                if (vertex != null) {
                    for (Object subOperator : ((AbstractOperator) ((AbstractProcessor) vertex.getProcessor())
                        .getOperator()).getNextOperators()) {
                        groupType = getCycleGroupType(null, (Operator) subOperator);
                        if (groupType != CycleGroupType.pipelined) {
                            break;
                        }
                    }
                }
                break;
            case SINGLE_WINDOW_SOURCE:
                groupType = CycleGroupType.windowed;
                break;
            case INC_VERTEX_CENTRIC_COMPUTE:
            case INC_VERTEX_CENTRIC_TRAVERSAL:
                groupType = CycleGroupType.incremental;
                break;
            case VERTEX_CENTRIC_COMPUTE:
            case VERTEX_CENTRIC_TRAVERSAL:
                groupType = CycleGroupType.statical;
                break;
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.operatorTypeNotSupportError(type.name()));
        }

        return groupType;
    }

    /**
     * Get max parallelism of vertex.
     */
    private int getMaxParallelism(PipelineVertex vertex) {
        int maxParallelism = vertex.getParallelism();
        switch (vertex.getType()) {
            case inc_process:
            case vertex_centric:
            case iterator:
                if (vertex.getOperator() instanceof AbstractStaticGraphVertexCentricOp) {
                    int shardNum = ((AbstractStaticGraphVertexCentricOp) vertex.getOperator()).getGraphViewDesc().getShardNum();
                    Preconditions.checkArgument(shardNum >= maxParallelism,
                        String.format("shardNum %d should not be less than maxParallelism %d", shardNum, maxParallelism));
                    return shardNum;
                }
                return MathUtil.minPowerOf2(maxParallelism);
            case inc_vertex_centric:
            case inc_iterator:
                return ((AbstractDynamicGraphVertexCentricOp) vertex.getOperator())
                    .getGraphViewDesc().getShardNum();
            default:
                return maxParallelism;
        }
    }

    private AffinityLevel buildGroupAffinityLevel(ExecutionVertexGroup vertexGroup) {
        AffinityLevel affinityLevel = null;
        for (ExecutionVertex vertex : vertexGroup.getVertexMap().values()) {
            if (affinityLevel == null) {
                affinityLevel = vertex.getAffinityLevel();
            } else {
                if (affinityLevel == vertex.getAffinityLevel()) {
                    continue;
                }
                // Set default value to worker.
                affinityLevel = AffinityLevel.worker;
                break;
            }
        }
        return affinityLevel;
    }

    private ExecutionEdge buildEdge(PipelineEdge pipelineEdge) {
        OutputType dataTransferType = pipelineEdge.getType();
        if (dataTransferType == null) {
            dataTransferType = OutputType.FORWARD;
        }
        return new ExecutionEdge(
            pipelineEdge.getPartition(),
            pipelineEdge.getEdgeId(),
            pipelineEdge.getEdgeName(),
            pipelineEdge.getSrcId(),
            pipelineEdge.getTargetId(),
            dataTransferType,
            pipelineEdge.getEncoder());
    }
}
