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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.api.collector.Collector;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.base.FlatMapFunction;
import org.apache.geaflow.api.function.base.MapFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.traversal.PGraphTraversal;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.rel.GraphAlgorithm;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.function.graph.source.DynamicGraphVertexScanSourceFunction;
import org.apache.geaflow.dsl.runtime.traversal.DagGroupBuilder;
import org.apache.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import org.apache.geaflow.dsl.runtime.traversal.StepLogicalPlan;
import org.apache.geaflow.dsl.runtime.traversal.StepLogicalPlanSet;
import org.apache.geaflow.dsl.runtime.traversal.StepLogicalPlanTranslator;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.InitParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.TraversalAll;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ConstantStartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ParameterStartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.StartId;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath;
import org.apache.geaflow.dsl.runtime.util.IDUtil;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphHelper;
import org.apache.geaflow.pipeline.job.IPipelineJobContext;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;

public class GeaFlowRuntimeGraph implements RuntimeGraph {

    private final QueryContext queryContext;

    private final IPipelineJobContext context;

    private final GeaFlowGraph graph;

    private final GraphSchema graphSchema;

    private final GraphViewDesc graphViewDesc;

    private final PGraphView<Object, Row, Row> graphView;

    private final StepLogicalPlanSet logicalPlanSet;

    public GeaFlowRuntimeGraph(QueryContext queryContext,
                               PGraphView<Object, Row, Row> graphView,
                               GeaFlowGraph graph,
                               StepLogicalPlanSet logicalPlanSet,
                               GraphViewDesc graphViewDesc) {
        this.queryContext = Objects.requireNonNull(queryContext);
        this.context = ((GeaFlowQueryEngine) queryContext.getEngineContext()).getPipelineContext();
        this.graphView = Objects.requireNonNull(graphView);
        this.graph = Objects.requireNonNull(graph);
        this.graphSchema = graph.getGraphSchema(queryContext.getGqlContext().getTypeFactory());
        this.logicalPlanSet = logicalPlanSet;
        this.graphViewDesc = graphViewDesc;
    }

    public GeaFlowRuntimeGraph(QueryContext queryContext,
                               PGraphView<Object, Row, Row> graphView,
                               GeaFlowGraph graph,
                               GraphViewDesc graphViewDesc) {
        this(queryContext, graphView, graph, planSet(graph, queryContext), graphViewDesc);
    }

    private static StepLogicalPlanSet planSet(GeaFlowGraph graph, QueryContext queryContext) {
        return new StepLogicalPlanSet(graph.getGraphSchema(queryContext.getGqlContext().getTypeFactory()));
    }

    @Override
    public <T> T getPlan() {
        return getPathTable().getPlan();
    }

    @Override
    public List<Path> take(IType<?> type) {
        return ArrayUtil.castList(getPathTable().take(logicalPlanSet.getMainPlan().getOutputPathSchema()));
    }

    @Override
    public RuntimeGraph traversal(GraphMatch graphMatch) {
        StepLogicalPlanTranslator planTranslator = new StepLogicalPlanTranslator();
        StepLogicalPlan logicalPlan = planTranslator.translate(graphMatch, logicalPlanSet);
        logicalPlanSet.setMainPlan(logicalPlan);
        return new GeaFlowRuntimeGraph(queryContext, graphView, graph, logicalPlanSet, graphViewDesc);
    }

    @Override
    public RuntimeTable getPathTable() {
        assert logicalPlanSet != null;
        DagGroupBuilder builder = new DagGroupBuilder();
        ExecuteDagGroup executeDagGroup = builder.buildExecuteDagGroup(logicalPlanSet);
        StepOperator<?, ?> mainOp = executeDagGroup.getMainDag().getEntryOperator();
        assert mainOp instanceof StepSourceOperator;
        Set<StartId> startIds = ((StepSourceOperator) mainOp).getStartIds();

        Set<ParameterStartId> parameterStartIds = startIds.stream()
            .filter(id -> id instanceof ParameterStartId)
            .map(id -> (ParameterStartId) id)
            .collect(Collectors.toSet());

        Set<Object> constantStartIds = startIds.stream()
            .filter(id -> id instanceof ConstantStartId)
            .map(id -> ((ConstantStartId) id).getValue())
            .collect(Collectors.toSet());

        int maxTraversal = context.getConfig().getInteger(DSLConfigKeys.GEAFLOW_DSL_MAX_TRAVERSAL);
        int dagMaxTraversal = executeDagGroup.getMaxIterationCount();

        boolean isAggTraversal = dagMaxTraversal == Integer.MAX_VALUE;
        if (!isAggTraversal) {
            maxTraversal = Math.max(0, Math.min(maxTraversal, dagMaxTraversal));
        }
        int parallelism = (queryContext.getTraversalParallelism() > 0
            && queryContext.getTraversalParallelism() <= graph.getShardCount())
            ? queryContext.getTraversalParallelism() : graph.getShardCount();

        PWindowStream<ITraversalResponse<ITreePath>> responsePWindow;

        assert graphView instanceof PIncGraphView : "Illegal graph view";
        queryContext.addMaterializedGraph(graph.getName());

        PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graph.getName());
        PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graph.getName());
        if (vertexStream == null && edgeStream == null) { // traversal on snapshot of the
            // dynamic graph
            PGraphWindow<Object, Row, Row> staticGraph = graphView.snapshot(graphViewDesc.getCurrentVersion());
            responsePWindow = staticGraphTraversal(staticGraph, parameterStartIds,
                constantStartIds, executeDagGroup, maxTraversal, isAggTraversal, parallelism);
        } else { // traversal on dynamic graph
            boolean enableIncrTraversal = DynamicGraphHelper.enableIncrTraversal(maxTraversal, startIds.size(),
                context.getConfig());
            if (maxTraversal != Integer.MAX_VALUE) {
                if (enableIncrTraversal) {
                    // Double the maxTraversal if is incrTraversal, need pre evolve subgraph.
                    // the evolve phase is 1 smaller than the query Iteration
                    maxTraversal = maxTraversal * 2 - 1;
                }
            }
            vertexStream = vertexStream != null ? vertexStream :
                queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                    .getPlan();
            edgeStream = edgeStream != null ? edgeStream :
                queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                    .getPlan();

            PIncGraphView<Object, Row, Row> dynamicGraph = graphView.appendGraph((PWindowStream) vertexStream,
                (PWindowStream) edgeStream);
            responsePWindow = dynamicGraphTraversal(dynamicGraph, parameterStartIds, constantStartIds, executeDagGroup,
                maxTraversal, isAggTraversal, parallelism, enableIncrTraversal);
        }
        responsePWindow.withParallelism(parallelism);
        PWindowStream<Row> resultPWindow = responsePWindow.flatMap(new ResponseToRowFunction())
            .withName(queryContext.createOperatorName("TraversalResponseToRow"));

        return new GeaFlowRuntimeTable(queryContext, context, resultPWindow);
    }

    private PWindowStream<ITraversalResponse<ITreePath>> staticGraphTraversal(
        PGraphWindow<Object, Row, Row> staticGraph,
        Set<ParameterStartId> parameterStartIds,
        Set<Object> constantStartIds,
        ExecuteDagGroup executeDagGroup,
        int maxTraversal,
        boolean isAggTraversal,
        int parallelism) {
        PWindowStream<ITraversalResponse<ITreePath>> responsePWindow;
        if (queryContext.getRequestTable() != null) { // traversal with request
            RuntimeTable requestTable = queryContext.getRequestTable();
            boolean isIdOnlyRequest = queryContext.isIdOnlyRequest();

            PWindowStream<Row> requestWindowStream = requestTable.getPlan();
            PWindowStream<ITraversalRequest<?>> parameterizedRequest;
            boolean isTraversalAllWithRequest;
            if (parameterStartIds.size() == 1) { // static request table attach the start id
                parameterizedRequest = requestWindowStream.map(
                    new RowToParameterRequestFunction(parameterStartIds.iterator().next(), isIdOnlyRequest));
                isTraversalAllWithRequest = false;
            } else { // static request table attach all the traversal ids.
                parameterizedRequest = requestWindowStream.map(new RowToParameterRequestFunction(null, isIdOnlyRequest))
                    .broadcast();
                isTraversalAllWithRequest = true;
            }
            responsePWindow =
                ((PGraphTraversal<Object, ITreePath>) getStaticVCTraversal(isAggTraversal,
                    staticGraph, executeDagGroup, maxTraversal, isTraversalAllWithRequest, parallelism))
                    .start((PWindowStream) parameterizedRequest);

        } else if (constantStartIds.size() > 0) { // static request with constant ids.
            responsePWindow =
                ((PGraphTraversal<Object, ITreePath>) getStaticVCTraversal(isAggTraversal,
                    staticGraph, executeDagGroup, maxTraversal, false, parallelism)).start(new ArrayList<>(constantStartIds));
        } else { // traversal all
            boolean enableTraversalAllSplit = queryContext.getGlobalConf()
                .getBoolean(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE);
            if (enableTraversalAllSplit) {
                DynamicGraphVertexScanSourceFunction<?> sourceFunction =
                    new DynamicGraphVertexScanSourceFunction<>(graphViewDesc);
                PWindowSource<?> source = queryContext.getEngineContext()
                    .createRuntimeTable(queryContext, sourceFunction)
                    .withParallelism(graphViewDesc.getShardNum())
                    .withName(queryContext.createOperatorName("VertexScanSource"));
                responsePWindow =
                    getStaticVCTraversal(isAggTraversal,
                        staticGraph, executeDagGroup, maxTraversal, false, parallelism)
                        .start((PWindowStream) source);
            } else {
                responsePWindow =
                    ((PGraphTraversal<Object, ITreePath>) getStaticVCTraversal(isAggTraversal,
                        staticGraph, executeDagGroup, maxTraversal, false, parallelism)).start();
            }
        }
        return responsePWindow;
    }

    private PWindowStream<ITraversalResponse<ITreePath>> dynamicGraphTraversal(
        PIncGraphView<Object, Row, Row> dynamicGraph, Set<ParameterStartId> parameterStartIds,
        Set<Object> constantStartIds, ExecuteDagGroup executeDagGroup, int maxTraversal, boolean isAggTraversal,
        int parallelism, boolean enableIncrTraversal) {
        if (queryContext.getRequestTable() != null) { // dynamic traversal with request
            RuntimeTable requestTable = queryContext.getRequestTable();
            boolean isIdOnlyRequest = queryContext.isIdOnlyRequest();

            PWindowStream<Row> requestWindowStream = requestTable.getPlan();
            PWindowStream<ITraversalRequest<?>> parameterizedRequest;
            boolean isTraversalAllWithRequest;
            if (parameterStartIds.size() == 1) { // request table attach the start id.
                parameterizedRequest = requestWindowStream.map(
                    new RowToParameterRequestFunction(parameterStartIds.iterator().next(), isIdOnlyRequest));
                isTraversalAllWithRequest = false;
            } else {
                parameterizedRequest = requestWindowStream.map(
                    new RowToParameterRequestFunction(null, isIdOnlyRequest)).broadcast();
                isTraversalAllWithRequest = true;
            }
            return ((PGraphTraversal<Object, ITreePath>) getDynamicVCTraversal(isAggTraversal, dynamicGraph,
                executeDagGroup, maxTraversal, isTraversalAllWithRequest, parallelism, enableIncrTraversal)).start(
                (PWindowStream) parameterizedRequest);
        } else if (constantStartIds.size() > 0) { // request with constant ids.
            return ((PGraphTraversal<Object, ITreePath>) getDynamicVCTraversal(isAggTraversal, dynamicGraph,
                executeDagGroup, maxTraversal, false, parallelism, enableIncrTraversal)).start(
                    new ArrayList<>(constantStartIds));
        } else { // dynamic traversal all
            boolean enableTraversalAllSplit = queryContext.getGlobalConf()
                .getBoolean(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE);
            if (enableTraversalAllSplit) {
                DynamicGraphVertexScanSourceFunction<?> sourceFunction = new DynamicGraphVertexScanSourceFunction<>(
                    graphViewDesc);
                PWindowSource<?> source = queryContext.getEngineContext()
                    .createRuntimeTable(queryContext, sourceFunction)
                    .withParallelism(graphViewDesc.getShardNum())
                    .withName(queryContext.createOperatorName("VertexScanSource"));
                return getDynamicVCTraversal(isAggTraversal, dynamicGraph, executeDagGroup, maxTraversal, false,
                    parallelism, enableIncrTraversal).start((PWindowStream) source);
            }
            return ((PGraphTraversal<Object, ITreePath>) getDynamicVCTraversal(isAggTraversal, dynamicGraph,
                executeDagGroup, maxTraversal, false, parallelism, enableIncrTraversal)).start();

        }
    }

    private PGraphTraversal<?, ?> getStaticVCTraversal(boolean isAggTraversal,
                                                       PGraphWindow<Object, Row, Row> staticGraph,
                                                       ExecuteDagGroup executeDagGroup, int maxTraversal,
                                                       boolean isTraversalAllWithRequest, int parallelism) {
        if (isAggTraversal) {
            return staticGraph.traversal(
                new GeaFlowStaticVCAggTraversal(executeDagGroup, maxTraversal, isTraversalAllWithRequest, parallelism));
        } else {
            return staticGraph.traversal(
                new GeaFlowStaticVCTraversal(executeDagGroup, maxTraversal, isTraversalAllWithRequest));
        }
    }

    private PGraphTraversal<?, ?> getDynamicVCTraversal(boolean isAggTraversal,
                                                        PIncGraphView<Object, Row, Row> dynamicGraph,
                                                        ExecuteDagGroup executeDagGroup, int maxTraversal,
                                                        boolean isTraversalAllWithRequest, int parallelism,
                                                        boolean enableIncrTraversal) {
        if (isAggTraversal) {
            return dynamicGraph.incrementalTraversal(
                new GeaFlowDynamicVCAggTraversal(executeDagGroup, maxTraversal, isTraversalAllWithRequest, parallelism));
        } else {
            return dynamicGraph.incrementalTraversal(
                new GeaFlowDynamicVCTraversal(executeDagGroup, maxTraversal, isTraversalAllWithRequest,
                    enableIncrTraversal));
        }
    }

    @Override
    public RuntimeTable runAlgorithm(GraphAlgorithm graphAlgorithm) {
        Class<? extends AlgorithmUserFunction> algorithmUserFunctionClass = graphAlgorithm.getUserFunctionClass();
        AlgorithmUserFunction algorithm;
        try {
            algorithm = algorithmUserFunctionClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Cannot new instance for class: " + algorithmUserFunctionClass.getName(), e);
        }
        int maxTraversal = context.getConfig().getInteger(DSLConfigKeys.GEAFLOW_DSL_MAX_TRAVERSAL);
        int parallelism = (queryContext.getTraversalParallelism() > 0
            && queryContext.getTraversalParallelism() <= graph.getShardCount())
            ? queryContext.getTraversalParallelism() : graph.getShardCount();

        PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graph.getName());
        PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graph.getName());
        PWindowStream<ITraversalResponse<Row>> responsePWindow;
        assert graphView instanceof PIncGraphView : "Illegal graph view";
        queryContext.addMaterializedGraph(graph.getName());
        if (vertexStream == null && edgeStream == null) { // traversal on snapshot of the dynamic graph
            PGraphWindow<Object, Row, Row> staticGraph = graphView.snapshot(graphViewDesc.getCurrentVersion());
            boolean enableAlgorithmSplit = algorithm instanceof IncrementalAlgorithmUserFunction;
            if (enableAlgorithmSplit) {
                DynamicGraphVertexScanSourceFunction<?> sourceFunction =
                    new DynamicGraphVertexScanSourceFunction<>(graphViewDesc);
                PWindowSource<?> source = queryContext.getEngineContext()
                    .createRuntimeTable(queryContext, sourceFunction)
                    .withParallelism(graphViewDesc.getShardNum())
                    .withName(queryContext.createOperatorName("VertexScanSource"));
                responsePWindow = staticGraph.traversal(new GeaFlowAlgorithmAggTraversal(
                        algorithm, maxTraversal, graphAlgorithm.getParams(), graphSchema, parallelism))
                    .start((PWindowStream) source);
            } else {
                responsePWindow = staticGraph.traversal(
                    new GeaFlowAlgorithmAggTraversal(algorithm, maxTraversal,
                        graphAlgorithm.getParams(), graphSchema, parallelism)).start();
            }
        } else { // traversal on dynamic graph
            vertexStream = vertexStream != null ? vertexStream :
                queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                    .getPlan();
            edgeStream = edgeStream != null ? edgeStream :
                queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                    .getPlan();

            PIncGraphView<Object, Row, Row> dynamicGraph = graphView.appendGraph((PWindowStream) vertexStream,
                (PWindowStream) edgeStream);
            boolean enableAlgorithmSplit = algorithm instanceof IncrementalAlgorithmUserFunction;
            if (enableAlgorithmSplit) {
                PWindowStream evolvedRequest =
                    vertexStream.map(new VertexToParameterRequestFunction()).union(
                        edgeStream.flatMap(new EdgeToParameterRequestFunction())).broadcast();
                responsePWindow = dynamicGraph.incrementalTraversal(
                    new GeaFlowAlgorithmDynamicAggTraversal(algorithm, maxTraversal,
                        graphAlgorithm.getParams(), graphSchema, parallelism)).start(evolvedRequest);
            } else {
                responsePWindow = dynamicGraph.incrementalTraversal(
                    new GeaFlowAlgorithmDynamicAggTraversal(algorithm, maxTraversal,
                        graphAlgorithm.getParams(), graphSchema, parallelism)).start();
            }
        }
        responsePWindow = responsePWindow.withParallelism(parallelism);
        PWindowStream<Row> resultPWindow = responsePWindow.flatMap(
            (FlatMapFunction<ITraversalResponse<Row>, Row>) (value, collector) -> collector.partition(
                value.getResponse()));
        return new GeaFlowRuntimeTable(queryContext, context, resultPWindow);
    }

    private static class ResponseToRowFunction implements FlatMapFunction<ITraversalResponse<ITreePath>, Row> {

        @Override
        public void flatMap(ITraversalResponse<ITreePath> value, Collector<Row> collector) {
            ITreePath treePath = value.getResponse();
            boolean isParametrizedTreePath = treePath instanceof ParameterizedTreePath;
            List<Path> paths = treePath.toList();
            for (Path path : paths) {
                Row resultRow = path;
                // If traversal with parameter request, we carry the parameter and requestId to the
                // sql function. So that the sql follow the match statement can refer the request parameter.
                if (isParametrizedTreePath) {
                    ParameterizedTreePath parameterizedTreePath = (ParameterizedTreePath) treePath;
                    Object requestId = parameterizedTreePath.getRequestId();
                    Row parameter = parameterizedTreePath.getParameter();
                    resultRow = new DefaultParameterizedRow(path, requestId, parameter);
                }
                collector.partition(resultRow);
            }
        }
    }

    private static class RowToParameterRequestFunction extends RichFunction
        implements MapFunction<Row, ITraversalRequest<?>> {

        private final ParameterStartId startId;

        private final boolean isIdOnlyRequest;

        private int numTasks;

        private int taskIndex;

        private long rowCounter = 0;

        public RowToParameterRequestFunction(ParameterStartId startId, boolean isIdOnlyRequest) {
            this.startId = startId;
            this.isIdOnlyRequest = isIdOnlyRequest;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            this.numTasks = runtimeContext.getTaskArgs().getParallelism();
            this.taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        }

        @Override
        public ITraversalRequest<?> map(Row row) {
            long requestId = IDUtil.uniqueId(numTasks, taskIndex, rowCounter);
            if (requestId < 0) {
                throw new GeaFlowDSLException("Request id exceed the Long.MAX, numTasks: "
                    + numTasks + ", taskIndex: " + taskIndex + ", rowCounter: " + rowCounter);
            }
            rowCounter++;
            Object vertexId;
            if (startId != null) {
                vertexId = startId.getIdExpression().evaluate(row);
            } else {
                vertexId = TraversalAll.INSTANCE;
            }
            if (isIdOnlyRequest) {
                return new IdOnlyRequest(vertexId);
            }
            return new InitParameterRequest(requestId, vertexId, row);
        }

        @Override
        public void close() {

        }
    }

    private static class VertexToParameterRequestFunction extends RichFunction
        implements MapFunction<RowVertex, ITraversalRequest<?>> {

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public ITraversalRequest<?> map(RowVertex vertex) {
            return new IdOnlyRequest(vertex.getId());
        }

        @Override
        public void close() {
        }
    }

    private static class EdgeToParameterRequestFunction extends RichFunction
        implements FlatMapFunction<RowEdge, ITraversalRequest<?>> {

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void flatMap(RowEdge edge, Collector<ITraversalRequest<?>> collector) {
            collector.partition(new IdOnlyRequest(edge.getSrcId()));
            collector.partition(new IdOnlyRequest(edge.getTargetId()));
        }

        @Override
        public void close() {
        }

    }
}
