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

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;
import static org.apache.geaflow.operator.Constants.GRAPH_VERSION;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.state.KeyValueState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowAlgorithmDynamicAggTraversalFunction
    implements IncVertexCentricAggTraversalFunction<Object, Row, Row, Object, Row, ITraversalAgg,
    ITraversalAgg>, RichIteratorFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowAlgorithmDynamicAggTraversalFunction.class);

    private static final String STATE_SUFFIX = "UpdatedValueState";

    private final AlgorithmUserFunction<Object, Object> userFunction;

    private final Object[] params;

    private GraphSchema graphSchema;

    private IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext;

    private GeaFlowAlgorithmDynamicRuntimeContext algorithmCtx;

    private MutableGraph<Object, Row, Row> mutableGraph;

    private transient Set<Object> initVertices;

    private transient KeyValueState<Object, Row> vertexUpdateValues;

    private boolean materializeInFinish;

    public GeaFlowAlgorithmDynamicAggTraversalFunction(GraphSchema graphSchema,
                                                       AlgorithmUserFunction<Object, Object> userFunction,
                                                       Object[] params) {
        this.graphSchema = Objects.requireNonNull(graphSchema);
        this.userFunction = Objects.requireNonNull(userFunction);
        this.params = Objects.requireNonNull(params);
        this.initVertices = new HashSet<>();
    }

    @Override
    public void open(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> vertexCentricFuncContext) {
        this.traversalContext = vertexCentricFuncContext;
        this.materializeInFinish = traversalContext.getRuntimeContext().getConfiguration().getBoolean(FrameworkConfigKeys.UDF_MATERIALIZE_GRAPH_IN_FINISH);
        this.algorithmCtx = new GeaFlowAlgorithmDynamicRuntimeContext(this, traversalContext,
            graphSchema);
        this.initVertices = new HashSet<>();
        this.userFunction.init(algorithmCtx, params);
        this.mutableGraph = traversalContext.getMutableGraph();

        int taskIndex = traversalContext.getRuntimeContext().getTaskArgs().getTaskIndex();
        String stateName = traversalContext.getTraversalOpName() + "_" + STATE_SUFFIX;
        KeyValueStateDescriptor descriptor = KeyValueStateDescriptor.build(
            stateName,
            traversalContext.getRuntimeContext().getConfiguration().getString(SYSTEM_STATE_BACKEND_TYPE));
        int parallelism = traversalContext.getRuntimeContext().getTaskArgs().getParallelism();
        int maxParallelism = traversalContext.getRuntimeContext().getTaskArgs().getMaxParallelism();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxParallelism);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);
        long recoverWindowId = traversalContext.getRuntimeContext().getWindowId();
        this.vertexUpdateValues = StateFactory.buildKeyValueState(descriptor,
            traversalContext.getRuntimeContext().getConfiguration());
        if (recoverWindowId > 1) {
            this.vertexUpdateValues.manage().operate().setCheckpointId(recoverWindowId - 1);
            this.vertexUpdateValues.manage().operate().recover();
        }
    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        if (!materializeInFinish) {
            Object vertexId = traversalRequest.getVId();
            algorithmCtx.setVertexId(vertexId);
            // The set formed by the vertices and source/target vertices of the edges inserted into
            // each window of the dynamic graph is taken as the trigger vertex for the first round of
            // iteration in the algorithm. These vertices may be duplicated, and needInit() returns
            // false when called after the first time to avoid redundant invocation.
            if (vertexId != null && needInit(vertexId)) {
                RowVertex vertex = (RowVertex) algorithmCtx.loadVertex();
                if (vertex != null) {
                    algorithmCtx.setVertexId(vertex.getId());
                    Row newValue = getVertexNewValue(vertex.getId());
                    userFunction.process(vertex, Optional.ofNullable(newValue), Collections.emptyIterator());
                }
            }
        }
    }

    public void updateVertexValue(Object vertexId, Row value) {
        vertexUpdateValues.put(vertexId, value);
    }

    public Row getVertexNewValue(Object vertexId) {
        return vertexUpdateValues.get(vertexId);
    }

    @Override
    public void evolve(Object vertexId, TemporaryGraph<Object, Row, Row> temporaryGraph) {
        if (!materializeInFinish) {
            IVertex<Object, Row> vertex = temporaryGraph.getVertex();
            List<IEdge<Object, Row>> edges = temporaryGraph.getEdges();
            materializeGraph(vertex, edges);
        } else {
            algorithmCtx.setVertexId(vertexId);
            RowVertex vertex = (RowVertex) temporaryGraph.getVertex();
            List<Object> emptyMessages = Collections.emptyList();
            userFunction.process(vertex, Optional.ofNullable(null), emptyMessages.iterator());
        }
    }

    private void materializeGraph(IVertex vertex, List<IEdge<Object, Row>> edges) {
        if (vertex != null) {
            mutableGraph.addVertex(GRAPH_VERSION, vertex);
        }
        if (edges != null) {
            for (IEdge<Object, Row> edge : edges) {
                mutableGraph.addEdge(GRAPH_VERSION, edge);
            }
        }
    }

    @Override
    public void compute(Object vertexId, Iterator<Object> messages) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex vertex;
        if (materializeInFinish) {
            vertex = (RowVertex) algorithmCtx.getIncVCTraversalCtx().getTemporaryGraph().getVertex();
            if (vertex == null) {
                vertex = (RowVertex) algorithmCtx.loadVertex();
            }
        } else {
            vertex = (RowVertex) algorithmCtx.loadVertex();
        }
        if (vertex != null) {
            Row newValue = getVertexNewValue(vertex.getId());
            userFunction.process(vertex, Optional.ofNullable(newValue), messages);
        }
    }

    @Override
    public void finish(Object vertexId, MutableGraph<Object, Row, Row> mutableGraph) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex graphVertex = (RowVertex) algorithmCtx.loadVertex();
        if (graphVertex != null) {
            Row newValue = getVertexNewValue(graphVertex.getId());
            userFunction.finish(graphVertex, Optional.ofNullable(newValue));
        }
        if (materializeInFinish) {
            IVertex vertex = algorithmCtx.getIncVCTraversalCtx().getTemporaryGraph().getVertex();
            materializeGraph(vertex, traversalContext.getTemporaryGraph().getEdges());
        }
    }

    public boolean needInit(Object v) {
        if (initVertices.contains(v)) {
            return false;
        } else {
            initVertices.add(v);
            return true;
        }
    }

    @Override
    public void finish() {
        algorithmCtx.finish();
        initVertices.clear();
        userFunction.finish();
        long windowId = traversalContext.getRuntimeContext().getWindowId();
        this.vertexUpdateValues.manage().operate().setCheckpointId(windowId);
        this.vertexUpdateValues.manage().operate().finish();
        this.vertexUpdateValues.manage().operate().archive();
    }


    @Override
    public void close() {
        algorithmCtx.close();
    }

    @Override
    public void initIteration(long iterationId) {
    }

    @Override
    public void finishIteration(long iterationId) {
        userFunction.finishIteration(iterationId);
    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.algorithmCtx.setAggContext(Objects.requireNonNull(aggContext));
    }
}
