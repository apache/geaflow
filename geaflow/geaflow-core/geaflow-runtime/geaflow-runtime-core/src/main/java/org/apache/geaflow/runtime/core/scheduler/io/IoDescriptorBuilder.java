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

package org.apache.geaflow.runtime.core.scheduler.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.cluster.response.IResult;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.shuffle.BatchPhase;
import org.apache.geaflow.common.shuffle.DataExchangeMode;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.core.graph.ExecutionEdge;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.processor.impl.AbstractProcessor;
import org.apache.geaflow.runtime.core.scheduler.cycle.CollectExecutionNodeCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import org.apache.geaflow.shuffle.ForwardOutputDesc;
import org.apache.geaflow.shuffle.InputDescriptor;
import org.apache.geaflow.shuffle.IoDescriptor;
import org.apache.geaflow.shuffle.OutputDescriptor;
import org.apache.geaflow.shuffle.RawDataInputDesc;
import org.apache.geaflow.shuffle.ResponseOutputDesc;
import org.apache.geaflow.shuffle.desc.IInputDesc;
import org.apache.geaflow.shuffle.desc.IOutputDesc;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.LogicalPipelineSliceMeta;
import org.apache.geaflow.shuffle.message.Shard;

public class IoDescriptorBuilder {

    public static int COLLECT_DATA_EDGE_ID = 0;

    public static IoDescriptor buildPrefetchIoDescriptor(ExecutionNodeCycle cycle, ExecutionNodeCycle childCycle, ExecutionTask childTask) {
        InputDescriptor inputDescriptor = buildPrefetchInputDescriptor(cycle, childCycle, childTask);
        return new IoDescriptor(inputDescriptor, null);
    }

    private static InputDescriptor buildPrefetchInputDescriptor(ExecutionNodeCycle cycle,
                                                                ExecutionNodeCycle childCycle,
                                                                ExecutionTask childTask) {
        ExecutionVertexGroup childVertexGroup = childCycle.getVertexGroup();
        List<Integer> inputEdgeIds = childVertexGroup.getVertexId2InEdgeIds().get(childTask.getVertexId());
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        Map<Integer, IInputDesc<?>> inputInfoList = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            if (edge == null) {
                continue;
            }
            if (edge.getType() != OutputType.FORWARD) {
                continue;
            }
            if (edge.getSrcId() == edge.getTargetId()) {
                continue;
            }
            IInputDesc<?> inputInfo = buildInputDesc(childTask, edge, childCycle, null, DataExchangeMode.BATCH, BatchPhase.PREFETCH_WRITE);
            if (inputInfo != null) {
                inputInfoList.put(edgeId, inputInfo);
            }
        }
        return new InputDescriptor(inputInfoList);
    }

    /**
     * Build pipeline io descriptor for input task.
     */
    public static IoDescriptor buildPipelineIoDescriptor(ExecutionTask task,
                                                         ExecutionNodeCycle cycle,
                                                         CycleResultManager resultManager,
                                                         boolean prefetch) {
        InputDescriptor inputDescriptor = buildPipelineInputDescriptor(task, cycle, resultManager, prefetch);
        OutputDescriptor outputDescriptor = buildPipelineOutputDescriptor(task, cycle, prefetch);
        return new IoDescriptor(inputDescriptor, outputDescriptor);
    }

    /**
     * Build pipeline input descriptor.
     */
    private static InputDescriptor buildPipelineInputDescriptor(ExecutionTask task,
                                                                ExecutionNodeCycle cycle,
                                                                CycleResultManager resultManager,
                                                                boolean prefetch) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());
        Map<Integer, IInputDesc<?>> inputInfoList = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            // Only build forward for pipeline.
            if (edge.getType() != OutputType.FORWARD) {
                continue;
            }
            if (edge.getSrcId() == edge.getTargetId()) {
                continue;
            }

            Tuple<DataExchangeMode, BatchPhase> tuple = getInputDataExchangeMode(cycle, task, edge.getSrcId(), prefetch);
            IInputDesc<?> inputInfo = buildInputDesc(task, edge, cycle, resultManager, tuple.f0, tuple.f1);
            if (inputInfo != null) {
                inputInfoList.put(edgeId, inputInfo);
            }
        }
        if (inputInfoList.isEmpty()) {
            return null;
        }
        return new InputDescriptor(inputInfoList);
    }

    /**
     * Build pipeline output descriptor.
     */
    private static OutputDescriptor buildPipelineOutputDescriptor(ExecutionTask task,
                                                                  ExecutionNodeCycle cycle,
                                                                  boolean prefetch) {
        int vertexId = task.getVertexId();
        if (cycle instanceof CollectExecutionNodeCycle) {
            return new OutputDescriptor(Collections.singletonList(buildCollectOutputDesc(task)));
        }
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(vertexId);
        List<IOutputDesc> outputDescList = new ArrayList<>();
        for (Integer edgeId : outEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            IOutputDesc outputDesc = buildOutputDesc(task, edge, cycle, prefetch);
            outputDescList.add(outputDesc);
        }
        if (outputDescList.isEmpty()) {
            return null;
        }
        return new OutputDescriptor(outputDescList);
    }

    public static IoDescriptor buildIterationIoDescriptor(ExecutionTask task,
                                                          ExecutionNodeCycle cycle,
                                                          CycleResultManager resultManager,
                                                          OutputType outputType) {
        InputDescriptor inputDescriptor = buildIterationInputDescriptor(task, cycle, resultManager, outputType);
        return new IoDescriptor(inputDescriptor, null);
    }

    private static InputDescriptor buildIterationInputDescriptor(ExecutionTask task,
                                                                 ExecutionNodeCycle cycle,
                                                                 CycleResultManager resultManager,
                                                                 OutputType outputType) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());
        Map<Integer, IInputDesc<?>> inputInfos = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            if (edge.getType() == outputType) {
                IInputDesc<?> inputInfo = buildInputDesc(task, edge, cycle, resultManager, DataExchangeMode.PIPELINE, BatchPhase.CLASSIC);
                if (inputInfo != null) {
                    inputInfos.put(edgeId, inputInfo);
                }
            }
        }
        return new InputDescriptor(inputInfos);
    }

    /**
     * Build input info for input task and edge.
     */
    protected static IInputDesc<Shard> buildInputDesc(ExecutionTask task,
                                                      ExecutionEdge inputEdge,
                                                      ExecutionNodeCycle cycle,
                                                      CycleResultManager resultManager,
                                                      DataExchangeMode dataExchangeMode,
                                                      BatchPhase batchPhase) {
        List<ExecutionTask> inputTasks = cycle.getVertexIdToTasks().get(inputEdge.getSrcId());
        int edgeId = inputEdge.getEdgeId();
        OutputType outputType = inputEdge.getType();

        switch (outputType) {
            case LOOP:
            case FORWARD:
                int vertexId = task.getVertexId();
                ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
                List<Shard> inputs = new ArrayList<>(inputTasks.size());
                if (dataExchangeMode == DataExchangeMode.BATCH && batchPhase == BatchPhase.CLASSIC) {
                    Map<Integer, List<Shard>> taskInputs =
                        DataExchanger.buildInput(vertexGroup.getVertexMap().get(vertexId), inputEdge, resultManager);
                    inputs = taskInputs.get(task.getIndex());
                } else {
                    for (ExecutionTask inputTask : inputTasks) {
                        LogicalPipelineSliceMeta logicalSlice = new LogicalPipelineSliceMeta(
                            inputTask.getIndex(),
                            task.getIndex(),
                            cycle.getPipelineId(),
                            edgeId,
                            inputTask.getWorkerInfo().getContainerName());
                        Shard shard = new Shard(edgeId, Collections.singletonList(logicalSlice));
                        inputs.add(shard);
                    }
                }
                return new ShardInputDesc(
                    edgeId,
                    inputEdge.getEdgeName(),
                    inputs,
                    inputEdge.getEncoder(),
                    dataExchangeMode,
                    batchPhase);
            case RESPONSE:
                List<IResult> results = resultManager.get(inputEdge.getEdgeId());
                if (results == null) {
                    return null;
                }
                List<Object> dataInput = new ArrayList<>();
                for (IResult result : results) {
                    if (result.getType() != OutputType.RESPONSE) {
                        throw new GeaflowRuntimeException(String.format("edge %s type %s not support handle result %s",
                            inputEdge.getEdgeId(), inputEdge.getType(), result.getType()));
                    }
                    dataInput.addAll(((ResponseResult) result).getResponse());
                }
                return new RawDataInputDesc(edgeId, inputEdge.getEdgeName(), dataInput);
            default:
                throw new GeaflowRuntimeException(String.format("not support build input for edge %s type %s",
                    inputEdge.getEdgeId(), inputEdge.getType()));
        }
    }

    private static IOutputDesc buildCollectOutputDesc(ExecutionTask task) {
        int opId = getCollectOpId((AbstractOperator) ((AbstractProcessor) task.getProcessor()).getOperator());
        ResponseOutputDesc outputDesc = new ResponseOutputDesc(opId,
            COLLECT_DATA_EDGE_ID, OutputType.RESPONSE.name());
        return outputDesc;

    }

    private static Integer getCollectOpId(AbstractOperator operator) {
        if (operator.getNextOperators().isEmpty()) {
            return operator.getOpArgs().getOpId();
        } else if (operator.getNextOperators().size() == 1) {
            return getCollectOpId((AbstractOperator) operator.getNextOperators().get(0));
        } else {
            throw new GeaflowRuntimeException("not support collect multi-output");
        }
    }

    protected static IOutputDesc buildOutputDesc(ExecutionTask task,
                                                 ExecutionEdge outEdge,
                                                 ExecutionNodeCycle cycle,
                                                 boolean prefetch) {

        int vertexId = task.getVertexId();
        switch (outEdge.getType()) {
            case LOOP:
            case FORWARD:
                List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(outEdge.getTargetId());
                List<Integer> taskIds = tasks.stream().map(ExecutionTask::getTaskId).collect(Collectors.toList());
                // TODO forward partitioner not write to all output tasks.
                int numPartitions = outEdge.getSrcId() == outEdge.getTargetId()
                    ? task.getMaxParallelism()
                    : task.getNumPartitions();
                DataExchangeMode dataExchangeMode = getOutputDataExchangeMode(cycle, outEdge.getTargetId(), prefetch);
                return new ForwardOutputDesc<>(
                    vertexId,
                    outEdge.getEdgeId(),
                    numPartitions,
                    outEdge.getEdgeName(),
                    dataExchangeMode,
                    taskIds,
                    outEdge.getPartitioner(),
                    outEdge.getEncoder());
            case RESPONSE:
                return new ResponseOutputDesc(
                    outEdge.getPartitioner().getOpId(),
                    outEdge.getEdgeId(),
                    outEdge.getEdgeName());
            default:
                throw new GeaflowRuntimeException(String.format("not support build output for edge %s type %s",
                    outEdge.getEdgeId(), outEdge.getType()));

        }
    }

    private static Tuple<DataExchangeMode, BatchPhase> getInputDataExchangeMode(ExecutionNodeCycle cycle,
                                                                                ExecutionTask task,
                                                                                int vertexId,
                                                                                boolean prefetch) {
        Map<Integer, ExecutionVertex> vertexMap = cycle.getVertexGroup().getVertexMap();
        DataExchangeMode dataExchangeMode = DataExchangeMode.BATCH;
        BatchPhase batchPhase = BatchPhase.CLASSIC;
        if (vertexMap.containsKey(vertexId)) {
            dataExchangeMode = DataExchangeMode.PIPELINE;
        } else if (prefetch && cycle.isHeadTask(task.getTaskId())) {
            batchPhase = BatchPhase.PREFETCH_READ;
        }
        return Tuple.of(dataExchangeMode, batchPhase);
    }

    private static DataExchangeMode getOutputDataExchangeMode(ExecutionNodeCycle cycle,
                                                              int vertexId,
                                                              boolean prefetch) {
        Map<Integer, ExecutionVertex> vertexMap = cycle.getVertexGroup().getVertexMap();
        return vertexMap.containsKey(vertexId) || prefetch ? DataExchangeMode.PIPELINE : DataExchangeMode.BATCH;
    }

}
