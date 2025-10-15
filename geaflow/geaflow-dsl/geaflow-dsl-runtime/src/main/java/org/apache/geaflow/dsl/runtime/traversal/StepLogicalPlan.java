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

package org.apache.geaflow.dsl.runtime.traversal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.common.types.VoidType;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepJoinFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepMapRowFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepNodeFilterFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepPathModifyFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepSortFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepSortFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.TraversalFromVertexFunction;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchEdgeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchVertexOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchVirtualEdgeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepDistinctOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepEndOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepExchangeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepFilterOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepGlobalAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepGlobalSingleValueAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepGlobalSortOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepJoinOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLocalAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLocalExchangeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLocalSingleValueAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLoopUntilOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepMapOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepMapRowOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepNodeFilterOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepReturnOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSortOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ConstantStartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.StartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSubQueryStartOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepUnionOperator;

/**
 * Logical plan for traversal step.
 */
public class StepLogicalPlan implements Serializable {

    private static final AtomicLong idCounter = new AtomicLong(0L);

    private List<StepLogicalPlan> inputs;

    private final StepOperator<?, ?> operator;

    private final List<StepLogicalPlan> outputs = new ArrayList<>();

    private boolean allowChain = false;

    public StepLogicalPlan(List<StepLogicalPlan> inputs, StepOperator<?, ?> operator) {
        this.operator = Objects.requireNonNull(operator);
        setInputs(inputs);
    }

    public StepLogicalPlan(StepLogicalPlan input, StepOperator<?, ?> operator) {
        this(input == null ? Collections.emptyList() : Collections.singletonList(input), operator);
    }

    public StepLogicalPlan(StepOperator<?, ?> operator) {
        this(Collections.emptyList(), operator);
    }

    @SuppressWarnings("unchecked")
    private void setInputs(List<StepLogicalPlan> inputs) {
        this.inputs = Lists.newArrayList(Objects.requireNonNull(inputs));
        GraphSchema modifyGraphSchema = null;
        for (StepLogicalPlan input : inputs) {
            input.addOutput(this);
            input.operator.addNextOperator((StepOperator) operator);
            if (modifyGraphSchema == null) {
                modifyGraphSchema = input.operator.getModifyGraphSchema();
            } else {
                modifyGraphSchema = modifyGraphSchema.merge(input.operator.getModifyGraphSchema());
            }
        }
        // inherit modify graph schema from the input.
        operator.withModifyGraphSchema(modifyGraphSchema);
    }

    /**
     * Create a start operator without start ids which means traversal all.
     *
     * @return The logical plan.
     */
    public static StepLogicalPlan start() {
        return start(new HashSet<>());
    }

    public static StepLogicalPlan start(Object... ids) {
        StartId[] startIds = new StartId[ids.length];
        for (int i = 0; i < startIds.length; i++) {
            startIds[i] = new ConstantStartId(ids[i]);
        }
        return start(Sets.newHashSet(startIds));
    }

    /**
     * Create a start operator with start ids which will be the head operator in the DAG.
     *
     * @param startIds The start ids for traversal. Empty start ids means traversal all.
     * @return The logical plan.
     */
    public static StepLogicalPlan start(Set<StartId> startIds) {
        StepSourceOperator startOp = new StepSourceOperator(nextPlanId(), startIds);
        return new StepLogicalPlan(startOp);
    }

    public StepLogicalPlan end() {
        StepEndOperator operator = new StepEndOperator(nextPlanId());
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(VoidType.INSTANCE)
                ;
    }

    public static StepLogicalPlan subQueryStart(String queryName) {
        StepSubQueryStartOperator operator = new StepSubQueryStartOperator(nextPlanId(),
                queryName);
        return new StepLogicalPlan(Collections.emptyList(), operator);
    }

    public StepLogicalPlan vertexMatch(MatchVertexFunction function) {
        MatchVertexOperator operator = new MatchVertexOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema());
    }

    public StepLogicalPlan edgeMatch(MatchEdgeFunction function) {
        MatchEdgeOperator operator = new MatchEdgeOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema());
    }

    public StepLogicalPlan virtualEdgeMatch(MatchVirtualEdgeFunction function) {
        MatchVirtualEdgeOperator operator = new MatchVirtualEdgeOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema());
    }

    public StepLogicalPlan startFrom(String label) {
        int fieldIndex = getOutputPathSchema().indexOf(label);
        if (fieldIndex != -1) { // start from exist label.
            IType<?> fieldType = getOutputPathSchema().getType(fieldIndex);
            if (!(fieldType instanceof VertexType)) {
                throw new IllegalArgumentException(
                        "Only can start traversal from vertex, current type is: " + fieldType);
            }
            return this.virtualEdgeMatch(new TraversalFromVertexFunction(fieldIndex, fieldType))
                    .withGraphSchema(this.getGraphSchema())
                    .withInputPathSchema(this.getOutputPathSchema())
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(EdgeType.emptyEdge(getGraphSchema().getIdType()))
                    ;
        } else { // start from a new label.
            return this.getHeadPlan();
        }
    }

    public StepLogicalPlan filter(StepBoolFunction function) {
        StepFilterOperator operator = new StepFilterOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType())
                ;
    }

    public StepLogicalPlan filterNode(StepNodeFilterFunction function) {
        StepNodeFilterOperator operator = new StepNodeFilterOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType());
    }

    public StepLogicalPlan distinct(StepKeyFunction keyFunction) {
        StepDistinctOperator localOperator = new StepDistinctOperator(nextPlanId(), keyFunction);
        StepLogicalPlan localDistinct = new StepLogicalPlan(this, localOperator)
                .withName("StepLocalDistinct-" + localOperator.getId())
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType());

        StepLogicalPlan exchange = localDistinct.exchange(keyFunction);

        StepDistinctOperator globalOperator = new StepDistinctOperator(nextPlanId(), keyFunction);
        return new StepLogicalPlan(exchange, globalOperator)
                .withName("StepGlobalDistinct-" + globalOperator.getId())
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType());
    }

    public StepLogicalPlan loopUtil(StepLogicalPlan loopBody, StepBoolFunction utilCondition,
                                    int minLoopCount, int maxLoopCount,
                                    int loopStartPathFieldCount, int loopBodyPathFieldCount) {
        if (!loopBody.getOutputs().isEmpty()) {
            throw new IllegalArgumentException("loopBody should be the last node");
        }
        StepLogicalPlan bodyStart = loopBody.getHeadPlan();
        if (bodyStart.getOperator() instanceof StepSourceOperator) {
            throw new IllegalArgumentException("Loop body cannot be a StepSourceOperator");
        }
        // append loop body the current plan.
        bodyStart.setInputs(Collections.singletonList(this));
        StepLoopUntilOperator operator = new StepLoopUntilOperator(
                nextPlanId(),
                bodyStart.getId(),
                loopBody.getId(),
                utilCondition,
                minLoopCount,
                maxLoopCount,
                loopStartPathFieldCount,
                loopBodyPathFieldCount);
        List<StepLogicalPlan> inputs = new ArrayList<>();
        inputs.add(loopBody);
        if (minLoopCount == 0) {
            inputs.add(this);
        }
        return new StepLogicalPlan(inputs, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(loopBody.getOutputPathSchema());
    }

    public StepLogicalPlan map(StepPathModifyFunction function, boolean isGlobal) {
        StepMapOperator operator = new StepMapOperator(nextPlanId(), function, isGlobal);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                ;
    }

    public StepLogicalPlan mapRow(StepMapRowFunction function) {
        StepMapRowOperator operator = new StepMapRowOperator(nextPlanId(), function);
        return new StepLogicalPlan(this, operator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                ;
    }

    public StepLogicalPlan union(List<StepLogicalPlan> inputs) {
        StepUnionOperator operator = new StepUnionOperator(nextPlanId());

        List<StepLogicalPlan> totalInputs = new ArrayList<>();
        totalInputs.add(this);
        totalInputs.addAll(inputs);
        List<PathType> inputPathTypes = totalInputs.stream()
                .map(StepLogicalPlan::getOutputPathSchema)
                .collect(Collectors.toList());
        return new StepLogicalPlan(totalInputs, operator)
                .withGraphSchema(getGraphSchema())
                .withInputPathSchema(inputPathTypes)
                ;
    }

    public StepLogicalPlan exchange(StepKeyFunction keyFunction) {
        StepExchangeOperator exchange = new StepExchangeOperator(nextPlanId(), keyFunction);
        return new StepLogicalPlan(this, exchange)
                .withGraphSchema(getGraphSchema())
                .withInputPathSchema(getOutputPathSchema())
                .withOutputPathSchema(getOutputPathSchema())
                .withOutputType(getOutputType())
                ;
    }

    public StepLogicalPlan localExchange(StepKeyFunction keyFunction) {
        StepLocalExchangeOperator exchange = new StepLocalExchangeOperator(nextPlanId(), keyFunction);
        return new StepLogicalPlan(this, exchange)
                .withGraphSchema(getGraphSchema())
                .withInputPathSchema(getOutputPathSchema())
                .withOutputPathSchema(getOutputPathSchema())
                .withOutputType(getOutputType())
                ;
    }

    public StepLogicalPlan join(StepLogicalPlan right, StepKeyFunction leftKey,
                                StepKeyFunction rightKey, StepJoinFunction joinFunction,
                                PathType inputJoinPathSchema, boolean isLocalJoin) {
        StepLogicalPlan leftExchange = isLocalJoin
                ? this.localExchange(leftKey) : this.exchange(leftKey);
        StepLogicalPlan rightExchange = isLocalJoin
                ? right.localExchange(rightKey) : right.exchange(rightKey);

        List<PathType> joinInputPaths = Lists.newArrayList(leftExchange.getOutputPathSchema(),
                rightExchange.getOutputPathSchema());

        StepJoinOperator joinOperator = new StepJoinOperator(nextPlanId(), joinFunction,
                inputJoinPathSchema, joinInputPaths, isLocalJoin);
        return new StepLogicalPlan(Lists.newArrayList(leftExchange, rightExchange), joinOperator)
                .withGraphSchema(getGraphSchema())
                .withInputPathSchema(joinInputPaths)
                .withOutputType(VertexType.emptyVertex(getGraphSchema().getIdType()))
                ;
    }

    public StepLogicalPlan sort(StepSortFunction sortFunction) {
        StepSortOperator localSortOperator = new StepSortOperator(nextPlanId(), sortFunction);
        StepLogicalPlan localSortPlan = new StepLogicalPlan(this, localSortOperator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType());
        StepLogicalPlan exchangePlan = localSortPlan.exchange(new StepKeyFunctionImpl(new int[0], new IType[0]));
        StepSortFunction globalSortFunction = ((StepSortFunctionImpl) sortFunction).copy(true);
        StepGlobalSortOperator globalSortOperator = new StepGlobalSortOperator(nextPlanId(),
                globalSortFunction, this.getOutputType(), this.getOutputPathSchema());

        return new StepLogicalPlan(exchangePlan, globalSortOperator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType());
    }

    public StepLogicalPlan aggregate(StepAggregateFunction aggFunction) {
        StepLocalSingleValueAggregateOperator localAggOp = new StepLocalSingleValueAggregateOperator(nextPlanId(), aggFunction);
        IType<?> localAggOutputType = ObjectType.INSTANCE;
        StepLogicalPlan localAggPlan = new StepLogicalPlan(this, localAggOp)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(StructType.singleValue(localAggOutputType, false));

        StepLogicalPlan exchangePlan = localAggPlan.exchange(new StepKeyFunctionImpl(new int[0], new IType[0]));
        StepGlobalSingleValueAggregateOperator globalAggOp = new StepGlobalSingleValueAggregateOperator(nextPlanId(), localAggOutputType,
                aggFunction);

        return new StepLogicalPlan(exchangePlan, globalAggOp)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(exchangePlan.getOutputPathSchema())
                .withOutputPathSchema(PathType.EMPTY)
                ;
    }

    public StepLogicalPlan aggregate(PathType inputPath, PathType outputPath,
                                     StepKeyFunction keyFunction,
                                     StepAggregateFunction aggFn) {
        StepLocalAggregateOperator localAggOp = new StepLocalAggregateOperator(nextPlanId(),
                keyFunction, aggFn);
        StepLogicalPlan localAggPlan = new StepLogicalPlan(this, localAggOp)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(inputPath)
                .withOutputPathSchema(inputPath)
                .withOutputType(getOutputType());
        StepLogicalPlan exchangePlan = localAggPlan.exchange(keyFunction);
        StepGlobalAggregateOperator globalAggOp = new StepGlobalAggregateOperator(nextPlanId(),
                keyFunction, aggFn);

        return new StepLogicalPlan(exchangePlan, globalAggOp)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(outputPath)
                .withOutputPathSchema(outputPath)
                .withOutputType(this.getOutputType())
                ;
    }

    public StepLogicalPlan ret() {
        StepReturnOperator returnOperator = new StepReturnOperator(nextPlanId());
        return new StepLogicalPlan(this, returnOperator)
                .withGraphSchema(this.getGraphSchema())
                .withInputPathSchema(this.getOutputPathSchema())
                .withOutputPathSchema(this.getOutputPathSchema())
                .withOutputType(this.getOutputType())
                ;
    }

    private void addOutput(StepLogicalPlan output) {
        assert !outputs.contains(output) : "Output has already added";
        outputs.add(output);
    }

    public StepLogicalPlan withFilteredFileds(Set<RexFieldAccess> fields) {
        operator.withFilteredFields(fields);
        return this;
    }

    public StepLogicalPlan withName(String name) {
        operator.withName(name);
        return this;
    }

    public StepLogicalPlan withOutputPathSchema(PathType outputPath) {
        operator.withOutputPathSchema(outputPath);
        return this;
    }

    public StepLogicalPlan withInputPathSchema(List<PathType> inputPaths) {
        operator.withInputPathSchema(inputPaths);
        return this;
    }

    public StepLogicalPlan withInputPathSchema(PathType inputPath) {
        operator.withInputPathSchema(inputPath);
        return this;
    }

    public StepLogicalPlan withOutputType(IType<?> outputType) {
        operator.withOutputType(outputType);
        return this;
    }

    public StepLogicalPlan withGraphSchema(GraphSchema graphSchema) {
        operator.withGraphSchema(graphSchema);
        return this;
    }

    public StepLogicalPlan withModifyGraphSchema(GraphSchema modifyGraphSchema) {
        operator.withModifyGraphSchema(modifyGraphSchema);
        return this;
    }

    public PathType getOutputPathSchema() {
        return operator.getOutputPathSchema();
    }

    public PathType getInputPathSchema() {
        assert operator.getInputPathSchemas().size() == 1;
        return operator.getInputPathSchemas().get(0);
    }

    public List<PathType> getInputPathSchemas() {
        return operator.getInputPathSchemas();
    }

    public GraphSchema getGraphSchema() {
        return operator.getGraphSchema();
    }

    public GraphSchema getModifyGraphSchema() {
        return operator.getModifyGraphSchema();
    }

    public IType<?> getOutputType() {
        return operator.getOutputType();
    }

    public String getPlanDesc(boolean onlyContent) {
        StringBuilder graphviz = new StringBuilder();
        if (!onlyContent) {
            graphviz.append("digraph G {\n");
        }
        generatePlanEdge(graphviz, this, new HashSet<>());
        generatePlanVertex(graphviz, this, new HashSet<>());
        if (!onlyContent) {
            graphviz.append("}");
        }
        return graphviz.toString();
    }

    public String getPlanDesc() {
        return getPlanDesc(false);
    }

    private void generatePlanEdge(StringBuilder graphviz, StepLogicalPlan plan, Set<Long> visited) {
        if (visited.contains(plan.getId())) {
            return;
        }
        visited.add(plan.getId());
        for (StepLogicalPlan input : plan.getInputs()) {
            String edgeDesc = "";
            if (!input.isAllowChain()) {
                edgeDesc = "chain = false";
            }
            graphviz.append(String.format("%d -> %d [label= \"%s\"]\n", input.getId(), plan.getId(), edgeDesc));
            generatePlanEdge(graphviz, input, visited);
        }
    }

    private void generatePlanVertex(StringBuilder graphviz, StepLogicalPlan plan, Set<Long> visited) {
        if (visited.contains(plan.getId())) {
            return;
        }
        visited.add(plan.getId());

        String vertexStr = plan.getOperator().toString();
        graphviz.append(String.format("%d [label= \"%s\"]\n", plan.getId(), vertexStr));
        for (StepLogicalPlan input : plan.getInputs()) {
            generatePlanVertex(graphviz, input, visited);
        }
    }

    private static long nextPlanId() {
        return idCounter.getAndIncrement();
    }

    public boolean isAllowChain() {
        return allowChain;
    }

    public void setAllowChain(boolean allowChain) {
        this.allowChain = allowChain;
    }

    public List<StepLogicalPlan> getInputs() {
        return inputs;
    }

    public StepOperator getOperator() {
        return operator;
    }

    public List<StepLogicalPlan> getOutputs() {
        return outputs;
    }

    public long getId() {
        return operator.getId();
    }

    public List<StepLogicalPlan> getFinalPlans() {
        if (outputs.isEmpty()) {
            return Collections.singletonList(this);
        }
        Set<StepLogicalPlan> finalPlans = new LinkedHashSet<>();
        for (StepLogicalPlan output : outputs) {
            finalPlans.addAll(output.getFinalPlans());
        }
        return ImmutableList.copyOf(finalPlans);
    }

    public StepLogicalPlan getHeadPlan() {
        if (getInputs().isEmpty()) {
            return this;
        }
        StepLogicalPlan headPlan = null;
        for (StepLogicalPlan input : inputs) {
            if (headPlan == null) {
                headPlan = input.getHeadPlan();
            } else if (headPlan != input.getHeadPlan()) {
                throw new IllegalArgumentException("Illegal plan with multi-head plan");
            }
        }
        return headPlan;
    }

    public StepLogicalPlan copy() {
        Map<Long, StepLogicalPlan> copyPlanCache = new LinkedHashMap<>();
        for (StepLogicalPlan finalPlan : getFinalPlans()) {
            finalPlan.copy(copyPlanCache);
        }
        return copyPlanCache.values().iterator().next().getHeadPlan();
    }

    private StepLogicalPlan copy(Map<Long, StepLogicalPlan> copyPlanCache) {
        List<StepLogicalPlan> inputsCopy = inputs.stream()
                .map(input -> input.copy(copyPlanCache))
                .collect(Collectors.toList());

        if (copyPlanCache.containsKey(getId())) {
            return copyPlanCache.get(getId());
        }
        StepLogicalPlan copyPlan = new StepLogicalPlan(inputsCopy, operator.copy());
        copyPlanCache.put(getId(), copyPlan);
        return copyPlan;
    }

    public static void clearCounter() {
        idCounter.set(0);
    }
}
