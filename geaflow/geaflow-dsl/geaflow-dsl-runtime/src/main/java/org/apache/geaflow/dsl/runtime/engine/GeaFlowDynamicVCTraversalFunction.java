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

package org.apache.geaflow.dsl.runtime.engine;

import static org.apache.geaflow.operator.Constants.GRAPH_VERSION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.BroadcastId;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.message.EvolveVertexMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageBox;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchEdgeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.AbstractDynamicGraphVertexCentricTraversalOp.IncGraphVCTraversalCtxImpl;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphHelper;
import org.apache.geaflow.state.pushdown.filter.EmptyFilter;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowDynamicVCTraversalFunction implements
    IncVertexCentricTraversalFunction<Object, Row, Row, MessageBox, ITreePath>, RichIteratorFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowDynamicVCTraversalFunction.class);

    private final GeaFlowCommonTraversalFunction commonFunction;

    private MutableGraph<Object, Row, Row> mutableGraph;

    int queryMaxIteration = 0;

    List<IFilter> incrMessageEdgeFilter = new ArrayList<>();

    boolean enableIncrTraversal;

    Set<Object> evolveIds;

    public GeaFlowDynamicVCTraversalFunction(ExecuteDagGroup executeDagGroup, boolean isTraversalAllWithRequest,
                                             boolean enableIncrTraversal) {
        this.commonFunction = new GeaFlowCommonTraversalFunction(executeDagGroup, isTraversalAllWithRequest);
        this.queryMaxIteration = executeDagGroup.getMaxIterationCount();
        this.enableIncrTraversal = enableIncrTraversal;
        this.evolveIds = new HashSet<>();
    }

    @Override
    public void open(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> vertexCentricFuncContext) {
        TraversalRuntimeContext traversalRuntimeContext = new GeaFlowDynamicTraversalRuntimeContext(
            vertexCentricFuncContext);
        this.mutableGraph = vertexCentricFuncContext.getMutableGraph();
        ((IncGraphVCTraversalCtxImpl) vertexCentricFuncContext).setEnableIncrMatch(enableIncrTraversal);
        this.commonFunction.open(traversalRuntimeContext);

        setEdgeFilters();
        if (!incrMessageEdgeFilter.isEmpty()) {
            // remove the last edgeFilter since 1 hop is already included in the incr edges.
            incrMessageEdgeFilter.remove(incrMessageEdgeFilter.size() - 1);
            Collections.reverse(incrMessageEdgeFilter);
        }

    }

    void setEdgeFilters() {
        StepOperator<?, ?> step = commonFunction.getExecuteDagGroup().getMainDag().getEntryOperator();
        while (!step.getNextOperators().isEmpty()) {
            List<? extends StepOperator<?, ?>> nextOperators = step.getNextOperators();
            if (nextOperators.size() > 1) {
                // ignore multi branch case
                incrMessageEdgeFilter.clear();
                break;
            }

            if (step.getClass() == MatchEdgeOperator.class) {
                // set the evolveMessage edge directions according to the query.
                EdgeDirection direction = ((MatchEdgeOperator) step).getFunction().getDirection();
                switch (direction) {
                    case OUT:
                        incrMessageEdgeFilter.add(InEdgeFilter.getInstance());
                        break;
                    case IN:
                        incrMessageEdgeFilter.add(OutEdgeFilter.getInstance());
                        break;
                    default:
                        incrMessageEdgeFilter.add(EmptyFilter.getInstance());
                }
            }

            step = nextOperators.get(0);
        }
    }

    @Override
    public void evolve(Object vertexId, TemporaryGraph<Object, Row, Row> temporaryGraph) {
        IVertex<Object, Row> vertex = temporaryGraph.getVertex();
        if (vertex != null) {
            mutableGraph.addVertex(GRAPH_VERSION, vertex);
        }
        List<IEdge<Object, Row>> edges = temporaryGraph.getEdges();
        if (edges != null) {
            for (IEdge<Object, Row> edge : edges) {
                mutableGraph.addEdge(GRAPH_VERSION, edge);
            }
        }

        evolveIds.add(vertexId);
    }

    @Override
    public void initIteration(long windowId) {

    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        commonFunction.init(traversalRequest);
    }

    @Override
    public void finish() {
        evolveIds.clear();
    }

    @Override
    public void close() {

    }

    private void sendEvolveMessage(Object vertexId, TraversalRuntimeContext context) {
        context.setVertex(IdOnlyVertex.of(vertexId));

        IFilter edgeFilter = incrMessageEdgeFilter.isEmpty() ? EmptyFilter.getInstance() : incrMessageEdgeFilter.get(
            (int) (context.getIterationId() - 1));
        EdgeGroup rowEdges = context.loadEdges(edgeFilter);
        StepOperator<StepRecord, StepRecord> operator = commonFunction.getExecuteDagGroup()
            .getMainDag()
            .getEntryOperator();
        for (RowEdge edge : rowEdges) {
            context.sendMessage(edge.getTargetId(), new EvolveVertexMessage(), operator.getId());
        }
    }


    private boolean needIncrTraversal() {
        // if has initRequests, no need send EvolveMessage.
        return enableIncrTraversal && DynamicGraphHelper.enableIncrTraversalRuntime(
            commonFunction.getContext().getRuntimeContext());
    }

    @Override
    public void compute(Object vertexId, Iterator<MessageBox> messageIterator) {
        TraversalRuntimeContext context = commonFunction.getContext();
        if (needIncrTraversal() && !(vertexId instanceof BroadcastId)) {
            long iterationId = context.getIterationId();
            // sendEvolveMessage to evolve subGraphs when iterationId is less than the plan iteration
            if (iterationId < queryMaxIteration - 1) {
                evolveIds.add(vertexId);
                sendEvolveMessage(vertexId, context);
                return;
            }

            if (iterationId == queryMaxIteration - 1) {
                // the current iteration is the end of evolve phase.
                evolveIds.add(vertexId);
                return;
            }
            // traversal
            commonFunction.compute(vertexId, messageIterator);

        } else {
            commonFunction.compute(vertexId, messageIterator);
        }
    }


    @Override
    public void finishIteration(long iterationId) {
        TraversalRuntimeContext context = commonFunction.getContext();
        if (needIncrTraversal()) {
            if (iterationId == 1) {
                // begin evolve
                for (ITraversalRequest<Object> request : commonFunction.getInitRequests()) {
                    Object vertexId = request.getVId();
                    sendEvolveMessage(vertexId, context);
                }
                commonFunction.getInitRequests().clear();
            }

            if (commonFunction.getContext().getIterationId() == queryMaxIteration - 1) {
                // begin Traversal
                LOGGER.info("StartEvolveIds: {}, {}, {}", evolveIds.size(), queryMaxIteration - 1,
                    context.getRuntimeContext().getWindowId());
                for (Object evolveId : evolveIds) {
                    ExecuteDagGroup executeDagGroup = commonFunction.getExecuteDagGroup();
                    executeDagGroup.execute(evolveId, executeDagGroup.getEntryOpId());
                }

            }

        } else {
            commonFunction.finish(iterationId);
        }

    }

    @Override
    public void finish(Object vertexId, MutableGraph<Object, Row, Row> mutableGraph) {
    }

}
