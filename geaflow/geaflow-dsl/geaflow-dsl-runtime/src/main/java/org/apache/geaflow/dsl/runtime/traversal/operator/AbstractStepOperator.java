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

package org.apache.geaflow.dsl.runtime.traversal.operator;

import static org.apache.geaflow.common.utils.ArrayUtil.castList;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedPath;
import org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.subquery.CallContext;
import org.apache.geaflow.dsl.runtime.expression.subquery.CallQueryExpression.CallState;
import org.apache.geaflow.dsl.runtime.expression.subquery.CallQueryProxy;
import org.apache.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import org.apache.geaflow.dsl.runtime.function.graph.StepFunction;
import org.apache.geaflow.dsl.runtime.traversal.DagTopologyGroup;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.ChainOperatorCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepBroadcastCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepEndCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepNextCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepPathPruneCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepReturnCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepWaitCallQueryCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.util.SchemaUtil;
import org.apache.geaflow.metrics.common.MetricConstants;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.BlackHoleMetricGroup;
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStepOperator<FUNC extends StepFunction, IN extends StepRecord, OUT extends StepRecord>
    implements StepOperator<IN, OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStepOperator.class);

    protected final long id;

    protected String name;
    protected final FUNC function;
    private final Map<Long, List<EndOfData>> caller2ReceiveEods = new HashMap<>();
    protected List<PathType> inputPathSchemas;
    protected PathType outputPathSchema;
    protected IType<?> outputType;
    protected GraphSchema graphSchema;
    protected GraphSchema modifyGraphSchema;
    protected StepRecordType outputRecordType;
    protected TraversalRuntimeContext context;
    protected List<StepOperator<OUT, ?>> nextOperators = new ArrayList<>();
    protected StepCollector<OUT> collector;

    protected boolean needAddToPath;

    protected IType<?>[] addingVertexFieldTypes;

    protected String[] addingVertexFieldNames;

    protected List<CallQueryProxy> callQueryProxies;

    private final int numCallQueries;

    private CallContext callContext = null;

    private CallState callState = null;

    protected int numTasks;
    private int numReceiveEods;
    protected long numProcessRecords;
    protected boolean isGlobalEmptyCycle;

    protected MetricGroup metricGroup;
    private Counter inputCounter;
    private Counter outputCounter;
    private Meter inputTps;
    private Meter outputTps;
    private Histogram processRt;
    private Counter inputEodCounter;

    public AbstractStepOperator(long id, FUNC function) {
        this.id = id;
        this.name = generateName();
        this.function = Objects.requireNonNull(createCallQueryProxy(function));
        this.callQueryProxies = this.function.getCallQueryProxies();
        this.numCallQueries = this.callQueryProxies.stream()
            .map(proxy -> proxy.getQueryCalls().length)
            .reduce(Integer::sum).orElse(0);
        if (this.callQueryProxies.size() > 0) {
            this.callContext = new CallContext();
            this.callState = CallState.INIT;
        }
    }

    @SuppressWarnings("unchecked")
    private FUNC createCallQueryProxy(FUNC function) {
        List<Expression> rewriteExpressions = function.getExpressions().stream()
            .map(CallQueryProxy::from)
            .collect(Collectors.toList());
        return (FUNC) function.copy(rewriteExpressions);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(TraversalRuntimeContext context) {
        Preconditions.checkArgument(inputPathSchemas != null, "inputPathSchemas is null");
        Preconditions.checkArgument(outputPathSchema != null, "outputPathSchema is null");
        Preconditions.checkArgument(outputType != null, "outputType is null");

        if (context.getConfig().getBoolean(ExecutionConfigKeys.ENABLE_DETAIL_METRIC)) {
            metricGroup = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL);
        } else {
            metricGroup = BlackHoleMetricGroup.INSTANCE;
        }
        inputCounter = metricGroup.counter(MetricNameFormatter.stepInputRecordName(getName()));
        inputTps = metricGroup.meter(MetricNameFormatter.stepInputRowTpsName(getName()));
        outputCounter = metricGroup.counter(MetricNameFormatter.stepOutputRecordName(getName()));
        outputTps = metricGroup.meter(MetricNameFormatter.stepOutputRowTpsName(getName()));
        processRt = metricGroup.histogram(MetricNameFormatter.stepProcessTimeRtName(getName()));
        inputEodCounter = metricGroup.counter(MetricNameFormatter.stepInputEodName(getName()));

        this.context = context;
        this.numTasks = context.getNumTasks();
        this.numReceiveEods = getNumReceiveEods();
        this.numProcessRecords = 0L;
        this.isGlobalEmptyCycle = true;
        for (StepOperator<OUT, ?> nextOp : nextOperators) {
            nextOp.open(context);
        }
        if (nextOperators.isEmpty()) {
            if (context.getTopology().belongMainDag(id)) {
                assert this instanceof StepEndOperator;
                // The collectors are empty means current is the StepEndOperator for main dag, use the StepEndCollector.
                this.collector = (StepCollector<OUT>) new StepEndCollector(context);
            } else { // current is StepReturnOperator.
                assert this instanceof StepReturnOperator;
                this.collector = (StepCollector<OUT>) new StepReturnCollector(context, id);
            }
        } else {
            List<StepCollector<OUT>> outputCollectors = new ArrayList<>();
            for (StepOperator<?, ?> nextOp : nextOperators) {
                StepCollector<OUT> collector;
                if (context.getTopology().isChained(id, nextOp.getId())) {
                    collector = (StepCollector<OUT>) new ChainOperatorCollector(
                        castList(Collections.singletonList(nextOp)), context);
                } else {
                    collector = (StepCollector<OUT>) new StepNextCollector(id, nextOp.getId(), context);
                }
                outputCollectors.add(collector);
            }
            this.collector = new StepBroadcastCollector<>(outputCollectors);
        }
        if (callQueryProxies.size() > 0) {
            this.collector = new StepWaitCallQueryCollector<>(callQueryProxies.get(0), this.collector);
        }

        PathType concatInputType = concatInputPathType();
        if (concatInputType != null) {
            List<String> appendFields = new ArrayList<>();
            int[] outputPathFieldIndices = ArrayUtil.toIntArray(
                outputPathSchema.getFieldNames().stream().map(name -> {
                    int index = concatInputType.indexOf(name);
                    if (index != -1) {
                        return index;
                    }
                    // If the last output field is not exist in the input, then it is a new
                    // append field by MatchVertex Or MatchEdge Or MatchExtend operator, return the
                    // field index of outputPathSchema.
                    index = concatInputType.size() + appendFields.size();
                    appendFields.add(name);
                    return index;
                }).collect(Collectors.toList()));
            // If any of the input field is not exist in the output, then we should prune the output path.
            boolean needPathPrune = concatInputType.getFieldNames().stream()
                .anyMatch(input -> !outputPathSchema.contain(input));

            if (needPathPrune) {
                this.collector = new StepPathPruneCollector<>(context, this.collector,
                    outputPathFieldIndices);
            }
        }
        this.needAddToPath = this instanceof LabeledStepOperator
            && outputPathSchema.contain(((LabeledStepOperator) this).getLabel())
            && !isSubQueryStartLabel();

        List<TableField> addingVertexFields = getModifyGraphSchema().getAddingFields(graphSchema);
        this.addingVertexFieldTypes = addingVertexFields.stream()
            .map(TableField::getType)
            .collect(Collectors.toList())
            .toArray(new IType[]{});

        this.addingVertexFieldNames = addingVertexFields.stream()
            .map(TableField::getName)
            .collect(Collectors.toList())
            .toArray(new String[]{});

        function.open(context, new FunctionSchemas(inputPathSchemas, outputPathSchema,
            outputType, graphSchema, modifyGraphSchema, addingVertexFieldTypes, addingVertexFieldNames));
    }

    private boolean isSubQueryStartLabel() {
        boolean isSubDag = !context.getTopology().belongMainDag(id);
        if (!isSubDag) {
            return false;
        }
        if (!(this instanceof LabeledStepOperator)) {
            return false;
        }
        List<Long> inputOpIds = context.getTopology().getInputIds(id);
        return inputOpIds.size() == 1 && context.getTopology()
            .getOperator(inputOpIds.get(0)) instanceof StepSubQueryStartOperator;
    }

    private int getNumReceiveEods() {
        DagTopologyGroup topologyGroup = context.getTopology();
        List<Long> inputOpIds = topologyGroup.getInputIds(id);
        int numReceiveEods = 0;
        for (long inputOpId : inputOpIds) {
            if (topologyGroup.isChained(id, inputOpId)) {
                numReceiveEods += 1;
            } else {
                numReceiveEods += numTasks;
            }
        }
        return numReceiveEods;
    }

    protected PathType concatInputPathType() {
        if (inputPathSchemas.isEmpty()) {
            return null;
        }
        if (inputPathSchemas.size() == 1) {
            return inputPathSchemas.get(0);
        }
        throw new IllegalArgumentException(
            this.getClass().getSimpleName() + " should override concatInputPathType() method.");
    }

    public final void process(IN record) {
        if (callState == CallState.FINISH) {
            throw new IllegalArgumentException("task index:" + context.getTaskIndex()
                + ", op id: " + id + " in illegal call state: " + callState);
        }
        long startTs = System.nanoTime();

        // set current operator id.
        context.setCurrentOpId(id);
        if (record.getType() == StepRecordType.EOD) {
            inputEodCounter.inc();
            EndOfData eod = (EndOfData) record;
            processEod(eod);
        } else {
            if (callState == CallState.INIT) {
                setCallState(CallState.CALLING);
            } else if (callState == CallState.WAITING) {
                setCallState(CallState.RETURNING);
            }
            // set current vertex.
            if (record.getType() == StepRecordType.VERTEX) {
                VertexRecord vertexRecord = (VertexRecord) record;
                RowVertex vertex = vertexRecord.getVertex();
                context.setVertex(vertex);
                if (callContext != null) {
                    callContext.addPath(context.getRequestId(), vertex.getId(),
                        vertexRecord.getPathById(vertex.getId()));
                    callContext.addRequest(vertex.getId(), context.getRequest());
                }
            } else {
                assert callContext == null : "Calling sub query on non-vertex record is not allowed.";
            }
            numProcessRecords++;
            processRecord(record);
            processRt.update((System.nanoTime() - startTs) / 1000L);
            inputCounter.inc();
            inputTps.mark();
        }
    }

    protected void processEod(EndOfData eod) {
        caller2ReceiveEods.computeIfAbsent(eod.getCallOpId(), k -> new ArrayList<>())
            .add(eod);
        boolean inputEmptyCycle = eod.isGlobalEmptyCycle;
        this.isGlobalEmptyCycle &= inputEmptyCycle;
        // Receive all EOD from input operators.
        for (Map.Entry<Long, List<EndOfData>> entry : caller2ReceiveEods.entrySet()) {
            long callerOpId = entry.getKey();
            List<EndOfData> receiveEods = entry.getValue();
            if (hasReceivedAllEod(receiveEods)) {
                LOGGER.info("Step op: {} task: {} received all eods. Iterations: {}",
                    this.getName(), context.getTaskIndex(), context.getIterationId());
                onReceiveAllEOD(callerOpId, receiveEods);
            }
        }
    }

    protected boolean hasReceivedAllEod(List<EndOfData> receiveEods) {
        if (numCallQueries > 0) {
            if (callQueryProxies.get(0).getCallState() == CallState.RETURNING
                || callQueryProxies.get(0).getCallState() == CallState.WAITING) {
                // When receiving all eod from the sub-query call, trigger the onReceiveAllEOD.
                return (numTasks * numCallQueries) == receiveEods.size();
            } else if (callQueryProxies.get(0).getCallState() == CallState.CALLING
                || callQueryProxies.get(0).getCallState() == CallState.INIT) {
                return numReceiveEods == receiveEods.size();
            }
            return false;
        } else {
            // For source operator, the input is empty, so if it has received eod,
            // it will trigger the onReceiveAllEOD. For other operator,
            // the count of eod should equal to the input size.
            return numReceiveEods == receiveEods.size();
        }
    }

    protected abstract void processRecord(IN record);

    protected void onReceiveAllEOD(long callerOpId, List<EndOfData> receiveEods) {
        finish();
        // Send EOD to output operators.
        collectEOD(callerOpId);
        receiveEods.clear();

        if (callState == CallState.FINISH) {
            LOGGER.info("step operator: {} finished, task id is: {}", getName(), context.getTaskIndex());
            this.setCallState(CallState.INIT);
        }
    }

    private void setCallState(CallState callState) {
        this.callState = callState;
        for (CallQueryProxy callQueryProxy : callQueryProxies) {
            callQueryProxy.setCallState(callState);
        }
    }

    public void finish() {
        if (callQueryProxies.size() > 0) {
            switch (callState) {
                case INIT:
                case CALLING:
                    this.setCallState(CallState.WAITING);
                    // push call context to the stack when finish calling.
                    context.push(id, callContext);
                    break;
                case WAITING:
                case RETURNING:
                    this.setCallState(CallState.FINISH);
                    // pop call context from stack when all calling has returned from sub query
                    context.pop(id);
                    // reset call context after pop from the stack
                    callContext.reset();
                    break;
                default:
                    throw new GeaFlowDSLException("Illegal call state: {}", callState);
            }
            for (CallQueryProxy callQueryProxy : callQueryProxies) {
                callQueryProxy.finishCall();
            }
        }
        function.finish((StepCollector) collector);

        LOGGER.info("Step op: {} task: {} finished. Iterations: {}", this.getName(),
            this.getContext().getTaskIndex(), context.getIterationId());
    }

    protected void collect(OUT record) {
        context.setInputOperatorId(id);
        collector.collect(record);
        outputCounter.inc();
        outputTps.mark();
    }

    @SuppressWarnings("unchecked")
    protected void collectEOD(long callerOpId) {
        this.isGlobalEmptyCycle &= numProcessRecords == 0L;
        EndOfData eod = EndOfData.of(callerOpId, id);
        eod.isGlobalEmptyCycle = isGlobalEmptyCycle;
        collector.collect((OUT) eod);
        this.isGlobalEmptyCycle = true;
        this.numProcessRecords = 0L;
    }

    @Override
    public void close() {
    }

    @Override
    public void addNextOperator(StepOperator nextOperator) {
        if (nextOperators.contains(nextOperator)) {
            return;
        }
        this.nextOperators.add(nextOperator);
    }

    @Override
    public List<StepOperator<OUT, ?>> getNextOperators() {
        return this.nextOperators;
    }

    @Override
    public StepOperator<IN, OUT> withOutputPathSchema(PathType pathSchema) {
        this.outputPathSchema = Objects.requireNonNull(pathSchema);
        return this;
    }

    @Override
    public StepOperator<IN, OUT> withInputPathSchema(List<PathType> inputPaths) {
        this.inputPathSchemas = Objects.requireNonNull(inputPaths);
        return this;
    }

    @Override
    public StepOperator<IN, OUT> withOutputType(IType<?> outputType) {
        this.outputType = Objects.requireNonNull(outputType);
        if (outputType instanceof VertexType) {
            this.outputRecordType = StepRecordType.VERTEX;
        } else {
            this.outputRecordType = StepRecordType.EDGE_GROUP;
        }
        return this;
    }

    @Override
    public StepOperator<IN, OUT> withGraphSchema(GraphSchema graph) {
        this.graphSchema = Objects.requireNonNull(graph);
        return this;
    }

    @Override
    public StepOperator<IN, OUT> withModifyGraphSchema(GraphSchema modifyGraphSchema) {
        this.modifyGraphSchema = modifyGraphSchema;
        return this;
    }

    @Override
    public List<PathType> getInputPathSchemas() {
        return inputPathSchemas;
    }

    @Override
    public PathType getOutputPathSchema() {
        return outputPathSchema;
    }

    @Override
    public IType<?> getOutputType() {
        return outputType;
    }

    @Override
    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    @Override
    public GraphSchema getModifyGraphSchema() {
        if (modifyGraphSchema == null) {
            return graphSchema;
        }
        return modifyGraphSchema;
    }

    public StepOperator<IN, OUT> copy() {
        return copyInternal()
            .withGraphSchema(graphSchema)
            .withInputPathSchema(inputPathSchemas)
            .withOutputPathSchema(outputPathSchema)
            .withOutputType(outputType);
    }

    public FUNC getFunction() {
        return function;
    }

    public TraversalRuntimeContext getContext() {
        return context;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractStepOperator)) {
            return false;
        }
        AbstractStepOperator<?, ?, ?> that = (AbstractStepOperator<?, ?, ?>) o;
        return id == that.id;
    }

    @Override
    public List<String> getSubQueryNames() {
        return function.getCallQueryProxies().stream()
            .flatMap(proxy -> proxy.getSubQueryNames().stream())
            .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        List<String> subQueryNames = getSubQueryNames();
        if (subQueryNames.size() > 0) {
            str.append("[").append(StringUtils.join(subQueryNames, ",")).append("]");
        }
        return str.toString();
    }

    public abstract StepOperator<IN, OUT> copyInternal();

    protected RowVertex alignToOutputSchema(RowVertex vertex) {
        if (vertex instanceof IdOnlyVertex) {
            return vertex;
        }
        String label = vertex.getLabel();
        VertexType inputVertexType = getModifyGraphSchema().getVertex(label);
        VertexType outputVertexType = ((VertexType) getOutputType());
        return SchemaUtil.alignToVertexSchema(vertex, inputVertexType, outputVertexType);
    }

    protected RowEdge alignToOutputSchema(RowEdge edge) {
        String label = edge.getLabel();
        EdgeType inputEdgeType = getModifyGraphSchema().getEdge(label);
        EdgeType outputEdgeType = ((EdgeType) getOutputType());
        return SchemaUtil.alignToEdgeSchema(edge, inputEdgeType, outputEdgeType);
    }

    protected Row withParameter(Row row) {
        Row parameters = context.getParameters();
        if (parameters != null) {
            if (row instanceof Path) {
                return new DefaultParameterizedPath((Path) row, context.getRequestId(), context.getParameters());
            } else {
                return new DefaultParameterizedRow(row, context.getRequestId(), context.getParameters());
            }
        }
        return row;
    }

    @Override
    public long getId() {
        return id;
    }

    private String generateName() {
        String className = getClass().getSimpleName();
        return className.substring(0, className.length() - "Operator".length()) + "-" + getId();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StepOperator<IN, OUT> withName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }
}
