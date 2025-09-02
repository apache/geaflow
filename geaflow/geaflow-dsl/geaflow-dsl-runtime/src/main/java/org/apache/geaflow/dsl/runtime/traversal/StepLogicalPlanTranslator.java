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

import static org.apache.calcite.sql.SqlKind.DESCENDING;
import static org.apache.geaflow.common.utils.ArrayUtil.toIntArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.BinaryUtil;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.rel.AbstractMatchNodeVisitor;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.PathModify.PathModifyExpression;
import org.apache.geaflow.dsl.rel.PathSort;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.LoopUntilMatch;
import org.apache.geaflow.dsl.rel.match.MatchAggregate;
import org.apache.geaflow.dsl.rel.match.MatchDistinct;
import org.apache.geaflow.dsl.rel.match.MatchExtend;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.MatchPathModify;
import org.apache.geaflow.dsl.rel.match.MatchPathSort;
import org.apache.geaflow.dsl.rel.match.MatchSamePredicate;
import org.apache.geaflow.dsl.rel.match.MatchUnion;
import org.apache.geaflow.dsl.rel.match.OptionalEdgeMatch;
import org.apache.geaflow.dsl.rel.match.OptionalVertexMatch;
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rel.match.VirtualEdgeMatch;
import org.apache.geaflow.dsl.rex.MatchAggregateCall;
import org.apache.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.ExpressionTranslator;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.field.ParameterFieldExpression;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggExpressionFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggExpressionFunctionImpl.StepAggCall;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepBoolFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepJoinFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepJoinFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyExpressionFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepNodeTypeFilterFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepPathModifyFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepSortFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepSortFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField.ORDER;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.apache.geaflow.dsl.runtime.plan.PhysicAggregateRelNode;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchEdgeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchVertexOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLocalExchangeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLocalSingleValueAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepNodeFilterOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ConstantStartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ParameterStartId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.StartId;
import org.apache.geaflow.dsl.runtime.util.FilterPushDownUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.IFilter;

public class StepLogicalPlanTranslator {

    /**
     * Translate path pattern to {@link StepLogicalPlan}.
     *
     * @param graphMatch The path pattern to translate.
     * @return The last node of the {@link StepLogicalPlan}.
     */
    public StepLogicalPlan translate(GraphMatch graphMatch,
                                     StepLogicalPlanSet logicalPlanSet) {
        // do the plan translate.
        LogicalPlanTranslatorVisitor translator =
            new LogicalPlanTranslatorVisitor(logicalPlanSet);
        return translator.translate(graphMatch.getPathPattern());
    }

    /**
     * Translate the {@link RelNode} in graph match to {@link StepLogicalPlan}.
     **/
    private static class LogicalPlanTranslatorVisitor extends AbstractMatchNodeVisitor<StepLogicalPlan> {

        private final GraphSchema graphSchema;

        private final StepLogicalPlanSet logicalPlanSet;

        private final GraphSchema modifyGraphSchema;

        // label -> plan
        private Map<String, StepLogicalPlan> planCache = new HashMap<>();

        private StepLogicalPlan logicalPlanHead = null;

        private final Map<RelNode, RexNode> nodePushDownFilters;

        public LogicalPlanTranslatorVisitor(StepLogicalPlanSet logicalPlanSet) {
            this(logicalPlanSet, new HashMap<>());
        }

        private LogicalPlanTranslatorVisitor(StepLogicalPlanSet logicalPlanSet,
                                             Map<RelNode, RexNode> nodePushDownFilters) {
            this.graphSchema = logicalPlanSet.getGraphSchema();
            this.logicalPlanSet = Objects.requireNonNull(logicalPlanSet);
            this.modifyGraphSchema = graphSchema;
            this.nodePushDownFilters = Objects.requireNonNull(nodePushDownFilters);
        }

        public StepLogicalPlan translate(RelNode pathPattern) {
            return this.visit(pathPattern);
        }

        @Override
        public StepLogicalPlan visitVertexMatch(VertexMatch vertexMatch) {
            String label = vertexMatch.getLabel();
            RexNode filter = nodePushDownFilters.get(vertexMatch);
            // TODO use optimizer rule to push the filter to the vertex-match.
            if (vertexMatch.getPushDownFilter() != null) {
                filter = vertexMatch.getPushDownFilter();
            }
            Set<StartId> startIds = new HashSet<>();
            if (vertexMatch.getInput() == null && filter != null) {
                Set<RexNode> ids = GQLRexUtil.findVertexIds(filter, (VertexRecordType) vertexMatch.getNodeType());
                startIds = toStartIds(ids);
            } else if (!vertexMatch.getIdSet().isEmpty()) {
                startIds = vertexMatch.getIdSet().stream().map(id -> new ConstantStartId(id)).collect(
                    Collectors.toSet());
            }
            Set<BinaryString> nodeTypes = vertexMatch.getTypes().stream()
                .map(s -> (BinaryString) BinaryUtil.toBinaryForString(s))
                .collect(Collectors.toSet());

            // If this head label node has generated in other branch, just reuse it and push down the startIds.
            if (vertexMatch.getInput() == null && planCache.containsKey(label)) {
                StepLogicalPlan plan = planCache.get(label);
                // push start ids to StepSourceOperator
                assert plan.getInputs().size() == 1;
                if (plan.getInputs().get(0).getOperator() instanceof StepSourceOperator) {
                    StepSourceOperator sourceOp = (StepSourceOperator) plan.getInputs().get(0).getOperator();
                    sourceOp.joinStartId(startIds);
                }
                if (vertexMatch.getTypes().size() > 0) {
                    return plan.filterNode(new StepNodeTypeFilterFunction(nodeTypes));
                }
                return plan;
            }
            IType<?> nodeType = SqlTypeUtil.convertType(vertexMatch.getNodeType());
            // generate input plan.
            StepLogicalPlan input;
            if (vertexMatch.getInput() != null) {
                input = this.visit(vertexMatch.getInput());
            } else {
                if (logicalPlanHead == null) { // create start plan for the first time
                    input = StepLogicalPlan.start(startIds)
                        .withGraphSchema(graphSchema)
                        .withModifyGraphSchema(modifyGraphSchema)
                        .withInputPathSchema(PathType.EMPTY)
                        .withOutputPathSchema(PathType.EMPTY)
                        .withOutputType(nodeType);
                    logicalPlanHead = input;
                } else { // start from the exists start plan.
                    StepLogicalPlan startPlan = logicalPlanHead;
                    assert startPlan.getOperator() instanceof StepSourceOperator :
                        "Start plan should be StepSourceOperator";
                    // push startIds of this branch to the StepSourceOperator.
                    ((StepSourceOperator) startPlan.getOperator()).unionStartId(startIds);
                    input = startPlan;
                }
            }
            PathType outputPath = (PathType) SqlTypeUtil.convertType(vertexMatch.getPathSchema());
            boolean isOptionalMatch = vertexMatch instanceof OptionalVertexMatch
                && SqlTypeUtil.convertType(vertexMatch.getNodeType()) != null;
            MatchVertexFunction mvf = new MatchVertexFunctionImpl(nodeTypes, isOptionalMatch,
                label, vertexMatch.getIdSet());
            StepLogicalPlan plan = input.vertexMatch(mvf)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType);
            planCache.put(label, plan);
            return plan;
        }

        @Override
        public StepLogicalPlan visitEdgeMatch(EdgeMatch edgeMatch) {
            String label = edgeMatch.getLabel();
            if (planCache.containsKey(label)) {
                return planCache.get(label);
            }
            if (edgeMatch.getInput() == null) {
                throw new GeaFlowDSLException("Graph match should start from a vertex");
            }
            StepLogicalPlan input = this.visit(edgeMatch.getInput());

            IType<?> nodeType = SqlTypeUtil.convertType(edgeMatch.getNodeType());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(edgeMatch.getPathSchema());

            IFilter<?>[] pushDownFilter = null;
            RexNode filter = nodePushDownFilters.get(edgeMatch);
            if (filter != null) {
                // push down edge timestamp condition
                IFilter<?> tsRangeFilter = null;
                List<TimeRange> tsRanges = FilterPushDownUtil.findTsRange(filter,
                    (EdgeRecordType) edgeMatch.getNodeType()).stream().collect(Collectors.toList());
                if (!tsRanges.isEmpty()) {
                    for (TimeRange timeRange : tsRanges) {
                        if (tsRangeFilter != null) {
                            tsRangeFilter = tsRangeFilter.or(new EdgeTsFilter(timeRange));
                        } else {
                            tsRangeFilter = new EdgeTsFilter(timeRange);
                        }
                    }
                }
                if (tsRangeFilter != null) {
                    pushDownFilter = new IFilter[]{tsRangeFilter};
                }
            }
            Set<BinaryString> edgeTypes = edgeMatch.getTypes().stream()
                .map(s -> (BinaryString) BinaryUtil.toBinaryForString(s))
                .collect(Collectors.toSet());
            boolean isOptionalMatch = edgeMatch instanceof OptionalEdgeMatch
                && SqlTypeUtil.convertType(edgeMatch.getNodeType()) != null;
            MatchEdgeFunction mef =
                pushDownFilter == null ? new MatchEdgeFunctionImpl(edgeMatch.getDirection(),
                    edgeTypes, isOptionalMatch, label) :
                    new MatchEdgeFunctionImpl(edgeMatch.getDirection(), edgeTypes, isOptionalMatch,
                        label, pushDownFilter);

            StepLogicalPlan plan = input.edgeMatch(mef)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType);
            planCache.put(label, plan);
            return plan;
        }

        @Override
        public StepLogicalPlan visitVirtualEdgeMatch(VirtualEdgeMatch virtualEdgeMatch) {
            StepLogicalPlan input = this.visit(virtualEdgeMatch.getInput());
            PathRecordType inputPath = ((IMatchNode) virtualEdgeMatch.getInput()).getPathSchema();
            Expression targetId = ExpressionTranslator.of(inputPath, logicalPlanSet)
                .translate(virtualEdgeMatch.getTargetId());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(virtualEdgeMatch.getPathSchema());
            MatchVirtualEdgeFunction virtualEdgeFunction = new MatchVirtualEdgeFunctionImpl(targetId);
            return input.virtualEdgeMatch(virtualEdgeFunction)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(SqlTypeUtil.convertType(virtualEdgeMatch.getNodeType()));
        }

        @Override
        public StepLogicalPlan visitFilter(MatchFilter filter) {
            // push down filter condition
            nodePushDownFilters.put(filter.getInput(), filter.getCondition());
            StepLogicalPlan input = this.visit(filter.getInput());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(filter.getPathSchema());
            PathRecordType inputPath = ((IMatchNode) filter.getInput()).getPathSchema();

            Expression condition =
                ExpressionTranslator.of(inputPath, logicalPlanSet).translate(filter.getCondition());
            StepBoolFunction fn = new StepBoolFunctionImpl(condition);
            return input.filter(fn).withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath);
        }

        @Override
        public StepLogicalPlan visitJoin(MatchJoin join) {
            JoinInfo joinInfo = join.analyzeCondition();
            PathRecordType leftPathType = ((IMatchNode) join.getLeft()).getPathSchema();
            PathRecordType rightPathType = ((IMatchNode) join.getRight()).getPathSchema();

            IType<?>[] leftKeyTypes = joinInfo.leftKeys.stream()
                .map(index ->
                    SqlTypeUtil.convertType(leftPathType
                        .getFieldList().get(index).getType()))
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            IType<?>[] rightKeyTypes = joinInfo.rightKeys.stream()
                .map(index ->
                    SqlTypeUtil.convertType(rightPathType
                        .getFieldList().get(index).getType()))
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            StepKeyFunction leftKeyFn = new StepKeyFunctionImpl(toIntArray(joinInfo.leftKeys), leftKeyTypes);
            StepKeyFunction rightKeyFn = new StepKeyFunctionImpl(toIntArray(joinInfo.rightKeys), rightKeyTypes);

            StepLogicalPlan leftPlan = visit(join.getLeft());
            StepLogicalPlan rightPlan = visit(join.getRight());
            IType<?>[] leftPathTypes = leftPlan.getOutputPathSchema().getTypes();
            IType<?>[] rightPathTypes = rightPlan.getOutputPathSchema().getTypes();

            Expression joinConditionExp =
                ExpressionTranslator.of(join.getPathSchema()).translate(join.getCondition());
            StepJoinFunction joinFunction = new StepJoinFunctionImpl(join.getJoinType(),
                leftPathTypes, rightPathTypes, joinConditionExp);

            PathType inputJoinPath = (PathType) SqlTypeUtil.convertType(leftPathType.join(rightPathType,
                join.getCluster().getTypeFactory()));
            PathType joinOutputPath = (PathType) SqlTypeUtil.convertType(join.getPathSchema());

            List<StepLogicalPlan> leftChainableVertex =
                StepLogicalPlanTranslator.getChainableVertexMatch(leftPlan);
            List<StepLogicalPlan> rightChainableVertex =
                StepLogicalPlanTranslator.getChainableVertexMatch(rightPlan);
            boolean isLocalJoin = false;
            if (leftChainableVertex.size() == 1
                && rightChainableVertex.size() == 1
                && joinInfo.leftKeys.size() == 1 && joinInfo.rightKeys.size() == 1) {
                String leftVertexLabel = ((MatchVertexOperator) leftChainableVertex.get(0).getOperator()).getLabel();
                String rightVertexLabel = ((MatchVertexOperator) rightChainableVertex.get(0).getOperator()).getLabel();
                if (leftPathType.getFieldList().get(joinInfo.leftKeys.get(0)).getName().equals(leftVertexLabel)
                    && rightPathType.getFieldList().get(joinInfo.rightKeys.get(0)).getName().equals(rightVertexLabel)) {
                    isLocalJoin = true;
                }
            }
            return leftPlan
                .join(rightPlan, leftKeyFn, rightKeyFn, joinFunction, inputJoinPath, isLocalJoin)
                .withOutputPathSchema(joinOutputPath);
        }

        @Override
        public StepLogicalPlan visitDistinct(MatchDistinct distinct) {
            RelNode input = distinct.getInput(0);
            IType<?>[] types = ((IMatchNode) input).getPathSchema().getFieldList().stream()
                .map(field -> SqlTypeUtil.convertType(field.getType()))
                .collect(Collectors.toList()).toArray(new IType[]{});
            int[] keyIndices = new int[types.length];
            for (int i = 0, size = types.length; i < size; i++) {
                keyIndices[i] = i;
            }
            StepKeyFunction keyFunction = new StepKeyFunctionImpl(keyIndices, types);
            PathType distinctPathType = (PathType) SqlTypeUtil.convertType(distinct.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(distinct.getNodeType());
            return visit(input).distinct(keyFunction)
                .withOutputPathSchema(distinctPathType)
                .withOutputType(nodeType);
        }

        @Override
        public StepLogicalPlan visitUnion(MatchUnion union) {
            List<StepLogicalPlan> inputPlans = new ArrayList<>();

            for (int i = 0, size = union.getInputs().size(); i < size; i++) {
                // The input of union should not referer the plan cache generated by each other.
                // So we create a new plan cache for each input.
                Map<String, StepLogicalPlan> prePlanCache = planCache;
                planCache = new HashMap<>(planCache);
                inputPlans.add(visit(union.getInput(i)));
                // recover pre-plan cache.
                planCache = prePlanCache;
            }

            StepLogicalPlan firstPlan = inputPlans.get(0);
            PathType unionPathType = (PathType) SqlTypeUtil.convertType(union.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(union.getNodeType());

            StepLogicalPlan unionPlan = firstPlan.union(inputPlans.subList(1, inputPlans.size()))
                .withModifyGraphSchema(firstPlan.getModifyGraphSchema())
                .withOutputPathSchema(unionPathType)
                .withOutputType(nodeType);
            if (union.all) {
                return unionPlan;
            } else {
                IType<?>[] types = unionPlan.getOutputPathSchema().getFields().stream()
                    .map(TableField::getType)
                    .collect(Collectors.toList()).toArray(new IType[]{});
                int[] keyIndices = new int[types.length];
                for (int i = 0, size = types.length; i < size; i++) {
                    keyIndices[i] = i;
                }
                StepKeyFunction keyFunction = new StepKeyFunctionImpl(keyIndices, types);
                return unionPlan.distinct(keyFunction)
                    .withModifyGraphSchema(unionPlan.getModifyGraphSchema())
                    .withOutputPathSchema(unionPlan.getOutputPathSchema())
                    .withOutputType(unionPlan.getOutputType());
            }
        }

        @Override
        public StepLogicalPlan visitSamePredicate(MatchSamePredicate samePredicate) {
            // Convert same predicate to union + filter operation
            // First, create union of left and right path patterns
            List<StepLogicalPlan> inputPlans = new ArrayList<>();
            
            // Visit left path pattern
            Map<String, StepLogicalPlan> prePlanCache = planCache;
            planCache = new HashMap<>(planCache);
            inputPlans.add(visit(samePredicate.getLeft()));
            planCache = prePlanCache;
            
            // Visit right path pattern
            prePlanCache = planCache;
            planCache = new HashMap<>(planCache);
            inputPlans.add(visit(samePredicate.getRight()));
            planCache = prePlanCache;
            
            StepLogicalPlan firstPlan = inputPlans.get(0);
            PathType samePredicatePathType = (PathType) SqlTypeUtil.convertType(samePredicate.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(samePredicate.getNodeType());
            
            // Create union plan
            StepLogicalPlan unionPlan = firstPlan.union(inputPlans.subList(1, inputPlans.size()))
                .withModifyGraphSchema(firstPlan.getModifyGraphSchema())
                .withOutputPathSchema(samePredicatePathType)
                .withOutputType(nodeType);
            
            // Apply distinct if needed
            if (!samePredicate.isDistinct()) {
                IType<?>[] types = unionPlan.getOutputPathSchema().getFields().stream()
                    .map(TableField::getType)
                    .collect(Collectors.toList()).toArray(new IType[]{});
                int[] keyIndices = new int[types.length];
                for (int i = 0, size = types.length; i < size; i++) {
                    keyIndices[i] = i;
                }
                StepKeyFunction keyFunction = new StepKeyFunctionImpl(keyIndices, types);
                unionPlan = unionPlan.distinct(keyFunction)
                    .withModifyGraphSchema(unionPlan.getModifyGraphSchema())
                    .withOutputPathSchema(unionPlan.getOutputPathSchema())
                    .withOutputType(unionPlan.getOutputType());
            }
            
            // Apply the shared predicate condition as a filter
            ExpressionTranslator translator = ExpressionTranslator.of(samePredicate.getPathSchema());
            Expression condition = translator.translate(samePredicate.getCondition());
            
            return unionPlan.filter(new StepBoolFunctionImpl(condition))
                .withModifyGraphSchema(unionPlan.getModifyGraphSchema())
                .withOutputPathSchema(unionPlan.getOutputPathSchema())
                .withOutputType(unionPlan.getOutputType());
        }

        @Override
        public StepLogicalPlan visitLoopMatch(LoopUntilMatch loopMatch) {
            StepLogicalPlan loopStart = visit(loopMatch.getInput());
            StepLogicalPlan loopBody = visit(loopMatch.getLoopBody());
            for (StepLogicalPlan plan : loopBody.getFinalPlans()) {
                plan.withModifyGraphSchema(loopStart.getModifyGraphSchema());
            }
            ExpressionTranslator translator = ExpressionTranslator.of(loopMatch.getLoopBody().getPathSchema());
            Expression utilCondition = translator.translate(loopMatch.getUtilCondition());

            PathType outputPath = (PathType) SqlTypeUtil.convertType(loopMatch.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(loopMatch.getNodeType());
            int loopStartPathFieldCount = loopStart.getOutputPathSchema().size();
            int loopBodyPathFieldCount = loopBody.getOutputPathSchema().size() - loopStartPathFieldCount;
            return loopStart.loopUtil(loopBody, new StepBoolFunctionImpl(utilCondition),
                    loopMatch.getMinLoopCount(), loopMatch.getMaxLoopCount(),
                    loopStartPathFieldCount, loopBodyPathFieldCount)
                .withModifyGraphSchema(loopStart.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType)
                ;
        }

        @Override
        public StepLogicalPlan visitSubQueryStart(SubQueryStart subQueryStart) {
            PathType pathType = (PathType) SqlTypeUtil.convertType(subQueryStart.getPathSchema());

            return StepLogicalPlan.subQueryStart(subQueryStart.getQueryName())
                .withGraphSchema(graphSchema)
                .withInputPathSchema(pathType)
                .withOutputPathSchema(pathType)
                .withOutputType(SqlTypeUtil.convertType(subQueryStart.getNodeType()));
        }

        @Override
        public StepLogicalPlan visitPathModify(MatchPathModify pathModify) {
            StepLogicalPlan input = visit(pathModify.getInput());
            List<PathModifyExpression> modifyExpressions = pathModify.getExpressions();
            int[] updatePathIndices = new int[modifyExpressions.size()];
            Expression[] updateExpressions = new Expression[modifyExpressions.size()];

            ExpressionTranslator translator = ExpressionTranslator.of(pathModify.getInput().getRowType(),
                logicalPlanSet);
            for (int i = 0; i < modifyExpressions.size(); i++) {
                PathModifyExpression modifyExpression = modifyExpressions.get(i);
                updatePathIndices[i] = modifyExpression.getIndex();
                updateExpressions[i] = translator.translate(modifyExpression.getObjectConstruct());
            }
            IType<?>[] inputFieldTypes = input.getOutputPathSchema().getFields()
                .stream()
                .map(TableField::getType)
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            GraphSchema modifyGraphSchema = (GraphSchema) SqlTypeUtil.convertType(pathModify.getModifyGraphType());
            StepPathModifyFunction modifyFunction = new StepPathModifyFunction(updatePathIndices,
                updateExpressions, inputFieldTypes);
            boolean isGlobal = pathModify.getExpressions().stream().anyMatch(exp -> {
                return exp.getObjectConstruct().getVariableInfo().stream().anyMatch(VariableInfo::isGlobal);
            });
            return input.map(modifyFunction, isGlobal)
                .withGraphSchema(graphSchema)
                .withModifyGraphSchema(modifyGraphSchema)
                .withInputPathSchema(input.getOutputPathSchema())
                .withOutputPathSchema((PathType) SqlTypeUtil.convertType(pathModify.getRowType()))
                .withOutputType(input.getOutputType());
        }

        @Override
        public StepLogicalPlan visitExtend(MatchExtend matchExtend) {
            StepLogicalPlan input = visit(matchExtend.getInput());
            List<PathModifyExpression> modifyExpressions = matchExtend.getExpressions();
            int[] updatePathIndices = new int[modifyExpressions.size()];
            Expression[] updateExpressions = new Expression[modifyExpressions.size()];

            ExpressionTranslator translator = ExpressionTranslator.of(
                matchExtend.getInput().getRowType(), logicalPlanSet);
            int offset = 0;
            for (int i = 0; i < modifyExpressions.size(); i++) {
                PathModifyExpression modifyExpression = modifyExpressions.get(i);
                if (matchExtend.getRewriteFields().contains(modifyExpression.getLeftVar().getLabel())) {
                    updatePathIndices[i] = modifyExpression.getIndex();
                } else {
                    updatePathIndices[i] = input.getOutputPathSchema().size() + offset;
                    offset++;
                }
                updateExpressions[i] = translator.translate(modifyExpression.getObjectConstruct());
            }
            IType<?>[] inputFieldTypes = input.getOutputPathSchema().getFields()
                .stream()
                .map(TableField::getType)
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            GraphSchema modifyGraphSchema = (GraphSchema) SqlTypeUtil.convertType(matchExtend.getModifyGraphType());
            StepPathModifyFunction modifyFunction = new StepPathModifyFunction(updatePathIndices,
                updateExpressions, inputFieldTypes);
            return input.map(modifyFunction, false)
                .withGraphSchema(graphSchema)
                .withModifyGraphSchema(modifyGraphSchema)
                .withInputPathSchema(input.getOutputPathSchema())
                .withOutputPathSchema((PathType) SqlTypeUtil.convertType(matchExtend.getRowType()))
                .withOutputType(input.getOutputType());
        }

        @Override
        public StepLogicalPlan visitSort(MatchPathSort pathSort) {
            StepLogicalPlan input = visit(pathSort.getInput());
            SortInfo sortInfo = buildSortInfo(pathSort);
            StepSortFunction orderByFunction = new StepSortFunctionImpl(sortInfo);
            PathType inputPath = input.getOutputPathSchema();
            return input.sort(orderByFunction)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withInputPathSchema(inputPath)
                .withOutputPathSchema(inputPath).withOutputType(inputPath);
        }

        @Override
        public StepLogicalPlan visitAggregate(MatchAggregate matchAggregate) {
            StepLogicalPlan input = visit(matchAggregate.getInput());
            List<RexNode> groupList = matchAggregate.getGroupSet();
            RelDataType inputRelDataType = matchAggregate.getInput().getRowType();
            List<Expression> groupListExpressions = groupList.stream().map(rex ->
                ExpressionTranslator.of(inputRelDataType, logicalPlanSet).translate(rex)).collect(
                Collectors.toList());
            StepKeyFunction keyFunction = new StepKeyExpressionFunctionImpl(
                groupListExpressions.toArray(new Expression[0]),
                groupListExpressions.stream().map(Expression::getOutputType).toArray(IType<?>[]::new));

            List<MatchAggregateCall> aggCalls = matchAggregate.getAggCalls();
            List<StepAggCall> aggFnCalls = new ArrayList<>();
            for (MatchAggregateCall aggCall : aggCalls) {
                String name = aggCall.getName();
                Expression[] argFields = aggCall.getArgList().stream().map(
                    rex -> ExpressionTranslator.of(inputRelDataType, logicalPlanSet).translate(rex))
                    .toArray(Expression[]::new);
                IType<?>[] argFieldTypes = Arrays.stream(argFields).map(Expression::getOutputType)
                    .toArray(IType<?>[]::new);
                Class<? extends UDAF<?, ?, ?>> udafClass =
                    PhysicAggregateRelNode.findUDAF(aggCall.getAggregation(), argFieldTypes);
                StepAggCall functionCall = new StepAggCall(name, argFields, argFieldTypes, udafClass,
                    aggCall.isDistinct());
                aggFnCalls.add(functionCall);
            }

            List<IType<?>> aggOutputTypes = aggCalls.stream()
                .map(call -> SqlTypeUtil.convertType(call.getType()))
                .collect(Collectors.toList());
            int[] pathPruneIndices = inputRelDataType.getFieldList().stream().filter(
                f -> matchAggregate.getPathSchema().getFieldNames().contains(f.getName())
            ).map(RelDataTypeField::getIndex).mapToInt(Integer::intValue).toArray();
            IType<?>[] inputPathTypes = inputRelDataType.getFieldList().stream()
                .map(f -> SqlTypeUtil.convertType(f.getType())).toArray(IType<?>[]::new);
            IType<?>[] pathPruneTypes = matchAggregate.getPathSchema().getFieldList().stream()
                .map(f -> SqlTypeUtil.convertType(f.getType())).toArray(IType<?>[]::new);
            StepAggregateFunction aggFn = new StepAggExpressionFunctionImpl(pathPruneIndices,
                pathPruneTypes, inputPathTypes, aggFnCalls, aggOutputTypes);

            PathType inputPath = input.getOutputPathSchema();
            PathType outputPath = (PathType) SqlTypeUtil.convertType(matchAggregate.getRowType());
            return input.aggregate(inputPath, outputPath, keyFunction, aggFn);
        }

        private SortInfo buildSortInfo(PathSort sort) {
            SortInfo sortInfo = new SortInfo();
            ExpressionTranslator translator = ExpressionTranslator.of(sort.getRowType());
            for (RexNode fd : sort.getOrderByExpressions()) {
                OrderByField orderByField = new OrderByField();
                if (fd.getKind() == DESCENDING) {
                    orderByField.order = ORDER.DESC;
                } else {
                    orderByField.order = ORDER.ASC;
                }
                orderByField.expression = translator.translate(fd);
                sortInfo.orderByFields.add(orderByField);
            }
            sortInfo.fetch = sort.getLimit() == null ? -1 :
                (int) TypeCastUtil.cast(
                    translator.translate(sort.getLimit()).evaluate(null),
                    Integer.class);
            return sortInfo;
        }
    }

    private static Set<StartId> toStartIds(Set<RexNode> ids) {
        return ids.stream()
            .map(StepLogicalPlanTranslator::toStartId)
            .collect(Collectors.toSet());
    }

    private static StartId toStartId(RexNode id) {
        List<RexNode> nonLiteralLeafNodes = GQLRexUtil.collect(id,
            child -> !(child instanceof RexCall) && !(child instanceof RexLiteral));

        Expression expression = ExpressionTranslator.of(null).translate(id);
        if (nonLiteralLeafNodes.isEmpty()) { // all the leaf node is constant.
            Object constantValue = expression.evaluate(null);
            return new ConstantStartId(constantValue);
        } else {
            Expression idExpression = expression.replace(exp -> {
                if (exp instanceof ParameterFieldExpression) {
                    ParameterFieldExpression field = (ParameterFieldExpression) exp;
                    return new FieldExpression(field.getFieldIndex(), field.getOutputType());
                }
                return exp;
            });
            return new ParameterStartId(idExpression);
        }
    }

    public static List<StepLogicalPlan> getChainableVertexMatch(StepLogicalPlan startPlan) {
        if (startPlan == null) {
            return Collections.emptyList();
        }
        if (startPlan.getOperator() instanceof MatchVertexOperator) {
            return Collections.singletonList(startPlan);
        } else if (startPlan.getOperator() instanceof MatchEdgeOperator
            || startPlan.getOperator() instanceof StepNodeFilterOperator
            || startPlan.getOperator() instanceof StepLocalExchangeOperator
            || startPlan.getOperator() instanceof StepLocalSingleValueAggregateOperator) {
            return startPlan.getInputs().stream().flatMap(
                input -> StepLogicalPlanTranslator.getChainableVertexMatch(input).stream()
            ).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
