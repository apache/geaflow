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

package org.apache.geaflow.operator.impl.graph.compute.dynamic;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;

import java.io.IOException;
import org.apache.geaflow.api.graph.base.algo.VertexCentricAlgo;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.CheckpointUtil;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDynamicGraphVertexCentricOp<K, VV, EV, M, FUNC extends VertexCentricAlgo<K, VV, EV, M>>
    extends AbstractGraphVertexCentricOp<K, VV, EV, M, FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractDynamicGraphVertexCentricOp.class);

    protected TemporaryGraphCache<K, VV, EV> temporaryGraphCache;
    private long checkpointDuration;

    public AbstractDynamicGraphVertexCentricOp(GraphViewDesc graphViewDesc, FUNC func) {
        super(graphViewDesc, func);
        assert !graphViewDesc.isStatic();
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_COMPUTE);
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
        this.maxIterations = this.function.getMaxIterationCount();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.temporaryGraphCache = new TemporaryGraphCache<>();
        this.checkpointDuration = runtimeContext.getConfiguration().getLong(BATCH_NUMBER_PER_CHECKPOINT);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} iterations:{} add vertex:{}",
                runtimeContext.getTaskArgs().getTaskId(),
                windowId,
                iterations,
                vertex);
        }
        this.temporaryGraphCache.addVertex(vertex);
        this.opInputMeter.mark();
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} iterations:{} add edge:{}",
                runtimeContext.getTaskArgs().getTaskId(),
                windowId,
                iterations,
                edge);
        }
        this.temporaryGraphCache.addEdge(edge);
        this.opInputMeter.mark();
    }

    @Override
    public void close() {
        this.temporaryGraphCache.clear();
        this.graphMsgBox.clearInBox();
        this.graphMsgBox.clearOutBox();
        this.graphState.manage().operate().close();
    }

    @Override
    protected GraphStateDescriptor<K, VV, EV> buildGraphStateDesc(String name) {
        GraphStateDescriptor<K, VV, EV> desc = super.buildGraphStateDesc(name);
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);
        desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        return desc;
    }

    public GraphViewDesc getGraphViewDesc() {
        return graphViewDesc;
    }


    protected void checkpoint() {
        if (CheckpointUtil.needDoCheckpoint(windowId, checkpointDuration)) {
            LOGGER.info("opName:{} do checkpoint for windowId:{}, checkpoint duration:{}",
                this.opArgs.getOpName(), windowId, checkpointDuration);
            long checkpoint = graphViewDesc.getCheckpoint(windowId);
            LOGGER.info("do checkpoint, checkpointId: {}", checkpoint);
            graphState.manage().operate().setCheckpointId(checkpoint);
            graphState.manage().operate().finish();
            graphState.manage().operate().archive();
        }
    }

    @Override
    protected void recover() {
        LOGGER.info("opName: {} will do recover, windowId: {}", this.opArgs.getOpName(),
            this.windowId);
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
            LOGGER.info("opName: {} do recover to state VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        } else {
            LOGGER.info("lastCheckPointId < 0");
            //If the graph has a checkpoint, should we recover it
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
}
