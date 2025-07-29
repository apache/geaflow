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

package org.apache.geaflow.operator.impl.graph.materialize;

import static org.apache.geaflow.operator.Constants.GRAPH_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.geaflow.api.graph.materialize.GraphMaterializeFunction;
import org.apache.geaflow.api.trait.CheckpointTrait;
import org.apache.geaflow.api.trait.TransactionTrait;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.base.window.AbstractOneInputOperator;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphViewMaterializeOp<K, VV, EV> extends AbstractOneInputOperator<Object,
    GraphMaterializeFunction<K, VV, EV>> implements TransactionTrait, CheckpointTrait {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphViewMaterializeOp.class);

    private final GraphViewDesc graphViewDesc;

    protected transient GraphState<K, VV, EV> graphState;

    public GraphViewMaterializeOp(GraphViewDesc graphViewDesc) {
        this.graphViewDesc = graphViewDesc;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        String name = graphViewDesc.getName();
        String storeType = graphViewDesc.getBackend().name();
        GraphStateDescriptor<K, VV, EV> descriptor = GraphStateDescriptor.build(
            graphViewDesc.getName(), storeType);
        descriptor.withDataModel(DataModel.DYNAMIC_GRAPH);
        descriptor.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        descriptor.withMetricGroup(runtimeContext.getMetric());

        int maxPara = graphViewDesc.getShardNum();
        int taskPara = runtimeContext.getTaskArgs().getParallelism();
        Preconditions.checkArgument(taskPara <= maxPara,
            String.format("task parallelism '%s' must be <= shard num(max parallelism) '%s'", taskPara, maxPara));

        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxPara,
            taskPara, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxPara);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);

        int taskId = runtimeContext.getTaskArgs().getTaskId();
        LOGGER.info("opName:{} taskId:{} taskIndex:{} keyGroup:{}", name, taskId,
            taskIndex, keyGroup);
        this.graphState = StateFactory.buildGraphState(descriptor, runtimeContext.getConfiguration());
        recover();
        this.function = new DynamicGraphMaterializeFunction<>(graphState);
    }

    @Override
    protected void process(Object record) throws Exception {
        if (record instanceof IVertex) {
            this.function.materializeVertex((IVertex<K, VV>) record);
        } else {
            this.function.materializeEdge((IEdge<K, EV>) record);
        }
    }

    @Override
    public void checkpoint(long windowId) {
        long checkpointId = graphViewDesc.getCheckpoint(windowId);
        this.graphState.manage().operate().setCheckpointId(checkpointId);
        this.graphState.manage().operate().finish();
        this.graphState.manage().operate().archive();
        LOGGER.info("do checkpoint over, checkpointId: {}", checkpointId);
    }

    @Override
    public void finish(long windowId) {

    }

    @Override
    public void rollback(long batchId) {
        recover(batchId);
    }

    @Override
    public void close() {
        if (this.graphState != null) {
            this.graphState.manage().operate().close();
        }
    }

    protected void recover() {
        recover(this.runtimeContext.getWindowId());
    }

    private void recover(long windowId) {
        long lastCheckPointId;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(),
                this.runtimeContext.getConfiguration());
            lastCheckPointId = keeper.getLatestViewVersion(graphViewDesc.getName());
            LOGGER.info("opName: {} will do recover, ViewMetaBookKeeper version: {}",
                this.opArgs.getOpName(), lastCheckPointId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        if (lastCheckPointId >= 0) {
            LOGGER.info("opName: {} do recover to graph VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        } else {
            //If the graph has a checkpoint, should we recover it
            LOGGER.info("lastCheckPointId < 0, windowId: {}", windowId);
            if (windowId > 1) {
                //Recover checkpoint id for dynamic graph is init graph version add windowId
                long recoverVersionId = graphViewDesc.getCheckpoint(windowId - 1);
                LOGGER.info("opName: {} do recover to latestVersionId: {}", this.opArgs.getOpName(),
                    recoverVersionId);
                graphState.manage().operate().setCheckpointId(recoverVersionId);
                graphState.manage().operate().recover();
            }
        }
    }

    public static class DynamicGraphMaterializeFunction<K, VV, EV> implements GraphMaterializeFunction<K, VV, EV> {

        private final GraphState<K, VV, EV> graphState;

        public DynamicGraphMaterializeFunction(GraphState<K, VV, EV> graphState) {
            this.graphState = graphState;
        }

        @Override
        public void materializeVertex(IVertex<K, VV> vertex) {
            graphState.dynamicGraph().V().add(GRAPH_VERSION, vertex);
        }

        @Override
        public void materializeEdge(IEdge<K, EV> edge) {
            graphState.dynamicGraph().E().add(GRAPH_VERSION, edge);
        }

    }

    @VisibleForTesting
    public GraphState<K, VV, EV> getGraphState() {
        return graphState;
    }
}
