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

package org.apache.geaflow.dsl.optimize.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.PathModify.PathModifyExpression;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchAggregate;
import org.apache.geaflow.dsl.rel.match.MatchExtend;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.OptionalEdgeMatch;
import org.apache.geaflow.dsl.rel.match.OptionalVertexMatch;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.MatchAggregateCall;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;
import org.apache.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinToGraphRule extends RelOptRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJoinToGraphRule.class);

    public AbstractJoinToGraphRule(RelOptRuleOperand operand) {
        super(operand);
    }

    /**
     * Determine if a Join in SQL can be converted to node matching or edge matching in GQL.
     */
    protected static GraphJoinType getJoinType(LogicalJoin join) {
        JoinInfo joinInfo = join.analyzeCondition();
        // only support inner join and equal-join currently.
        if (!joinInfo.isEqui() || !isSupportJoinType(join.getJoinType())) {
            return GraphJoinType.NONE_GRAPH_JOIN;
        }

        List<Integer> leftKeys = new ArrayList<>(joinInfo.leftKeys);
        List<Integer> rightKeys = new ArrayList<>(joinInfo.rightKeys);
        RelDataType leftType = join.getLeft().getRowType();
        RelDataType rightType = join.getRight().getRowType();

        GraphJoinType graphJoinType = GraphJoinType.NONE_GRAPH_JOIN;
        for (int i = 0; i < leftKeys.size(); i++) {
            Integer leftKey = leftKeys.get(i);
            Integer rightKey = rightKeys.get(i);
            GraphJoinType currentJoinType = getJoinType(leftKey, leftType, rightKey, rightType);
            if (currentJoinType != GraphJoinType.NONE_GRAPH_JOIN) {
                if (graphJoinType == GraphJoinType.NONE_GRAPH_JOIN) {
                    graphJoinType = currentJoinType;
                } else if (graphJoinType != currentJoinType) {
                    // contain multi join pattern, can not translate to graph, just return
                    return GraphJoinType.NONE_GRAPH_JOIN;
                }
            }
        }
        return graphJoinType;
    }

    protected static boolean isSupportJoinType(JoinRelType type) {
        return type == JoinRelType.INNER || type == JoinRelType.LEFT;
    }

    private static GraphJoinType getJoinType(int leftIndex, RelDataType leftType, int rightIndex,
                                             RelDataType rightType) {

        RelDataType leftKeyType = leftType.getFieldList().get(leftIndex).getType();
        RelDataType rightKeyType = rightType.getFieldList().get(rightIndex).getType();

        if (leftKeyType instanceof MetaFieldType && rightKeyType instanceof MetaFieldType) {
            MetaField leftMetaField = ((MetaFieldType) leftKeyType).getMetaField();
            MetaField rightMetaField = ((MetaFieldType) rightKeyType).getMetaField();

            switch (leftMetaField) {
                case VERTEX_ID:
                    if (rightMetaField == MetaField.EDGE_SRC_ID) {
                        return GraphJoinType.VERTEX_JOIN_EDGE;
                    } else if (rightMetaField == MetaField.EDGE_TARGET_ID) {
                        return GraphJoinType.EDGE_JOIN_VERTEX;
                    }
                    break;
                case EDGE_SRC_ID:
                    if (rightMetaField == MetaField.VERTEX_ID) {
                        return GraphJoinType.VERTEX_JOIN_EDGE;
                    }
                    break;
                case EDGE_TARGET_ID:
                    if (rightMetaField == MetaField.VERTEX_ID) {
                        return GraphJoinType.EDGE_JOIN_VERTEX;
                    }
                    break;
                default:
            }
        }
        return GraphJoinType.NONE_GRAPH_JOIN;
    }

    /**
     * Determine if an SQL logical RelNode belongs to a single-chain and can be rewritten as a GQL RelNode.
     */
    private static boolean isSingleChain(RelNode relNode) {
        return relNode instanceof LogicalFilter || relNode instanceof LogicalProject
            || relNode instanceof LogicalAggregate;
    }

    /**
     * Determine if an SQL logical RelNode is downstream of a TableScan and if all the RelNodes
     * on its input chain can be rewritten as GQL.
     */
    protected static boolean isSingleChainFromLogicalTableScan(RelNode node) {
        RelNode relNode = GQLRelUtil.toRel(node);
        if (isSingleChain(relNode)) {
            return relNode.getInputs().size() == 1 && isSingleChainFromLogicalTableScan(
                relNode.getInput(0));
        }
        return relNode instanceof LogicalTableScan;
    }

    /**
     * Determine if an SQL logical RelNode is downstream of a GraphMatch and if all the RelNodes
     * on its input chain can be rewritten as GQL.
     */
    protected static boolean isSingleChainFromGraphMatch(RelNode node) {
        RelNode relNode = GQLRelUtil.toRel(node);
        if (isSingleChain(relNode)) {
            return relNode.getInputs().size() == 1 && isSingleChainFromGraphMatch(
                relNode.getInput(0));
        }
        return relNode instanceof GraphMatch;
    }

    /**
     * Convert the RelNodes on the single chain from "from" to "to" into GQL and push them
     * into the input. Rebuild a MatchNode, and sequentially place all the accessible fields
     * into the returned rexNodeMap.
     */
    protected IMatchNode concatToMatchNode(RelBuilder builder, IMatchNode left, RelNode from, RelNode to,
                                           IMatchNode input, List<RexNode> rexNodeMap) {
        if (from instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode filterInput = from == to ? input : concatToMatchNode(builder, left,
                GQLRelUtil.toRel(filter.getInput()), to, input, inputRexNode2RexInfo);
            int lastNodeIndex = filterInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            RexNode newCondition = filter.getCondition();
            if (!inputRexNode2RexInfo.isEmpty()) {
                newCondition = GQLRexUtil.replace(filter.getCondition(), rexNode -> {
                    if (rexNode instanceof RexInputRef) {
                        return inputRexNode2RexInfo.get(((RexInputRef) rexNode).getIndex());
                    }
                    return rexNode;
                });
                rexNodeMap.addAll(inputRexNode2RexInfo);
            } else {
                String lastNodeLabel = filterInput.getPathSchema().getFieldList().get(lastNodeIndex)
                    .getName();
                RelDataType oldType = filterInput.getPathSchema().getFieldList().get(lastNodeIndex)
                    .getType();
                newCondition = GQLRexUtil.replace(newCondition, rex -> {
                    if (rex instanceof RexInputRef) {
                        return builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, oldType),
                            ((RexInputRef) rex).getIndex());
                    }
                    return rex;
                });
                rexNodeMap.addAll(inputRexNode2RexInfo);
            }
            return MatchFilter.create(filterInput, newCondition, filterInput.getPathSchema());
        } else if (from instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode projectInput = from == to ? input : concatToMatchNode(builder, left,
                GQLRelUtil.toRel(project.getInput()), to, input, inputRexNode2RexInfo);

            int lastNodeIndex = projectInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            String lastNodeLabel = projectInput.getPathSchema().getFieldList().get(lastNodeIndex)
                .getName();
            RelDataType lastNodeType =
                projectInput.getPathSchema().getFieldList().get(lastNodeIndex).getType();
            List<RexNode> replacedProjects = new ArrayList<>();
            //Rewrite the projects by the rex mapping table returned through input reconstructing.
            if (!inputRexNode2RexInfo.isEmpty()) {
                replacedProjects.addAll(project.getProjects().stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
                    if (rexNode instanceof RexInputRef) {
                        return inputRexNode2RexInfo.get(((RexInputRef) rexNode).getIndex());
                    }
                    return rexNode;
                })).collect(Collectors.toList()));
            } else {
                replacedProjects.addAll(project.getProjects().stream().map(prj -> GQLRexUtil.replace(prj, rex -> {
                    if (rex instanceof RexInputRef) {
                        return builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, lastNodeType),
                            ((RexInputRef) rex).getIndex());
                    }
                    return rex;
                })).collect(Collectors.toList()));
            }
            rexNodeMap.addAll(replacedProjects);

            List<RexNode> metaFieldProjects = replacedProjects.stream()
                .filter(rex -> rex instanceof RexFieldAccess).collect(Collectors.toList());
            List<RexNode> addFieldProjects = replacedProjects.stream()
                .filter(rex -> !metaFieldProjects.contains(rex)).collect(Collectors.toList());
            List<Integer> addFieldIndices = addFieldProjects.stream().map(replacedProjects::indexOf)
                .collect(Collectors.toList());

            EdgeRecordType edgeNewType;
            String edgeName;
            VertexRecordType vertexNewType;
            String vertexName;
            RelDataType oldType = projectInput.getPathSchema().firstField().get().getType();
            PathRecordType newPathRecordType = ((PathRecordType) projectInput.getRowType());
            int extendNodeIndex;
            String extendNodeLabel;
            List<String> addFieldNames;
            List<RexNode> operands = new ArrayList<>();
            Map<RexNode, VariableInfo> rex2VariableInfo = new HashMap<>();
            RexBuilder rexBuilder = builder.getRexBuilder();
            if (addFieldProjects.size() > 0) {
                //Paths start with a vertex, add fields on the vertex and add a vertex extension.
                if (oldType instanceof VertexRecordType) {
                    vertexNewType = (VertexRecordType) oldType;
                    addFieldNames = this.generateFieldNames("f", addFieldProjects.size(),
                        new HashSet<>(vertexNewType.getFieldNames()));
                    for (int i = 0; i < addFieldNames.size(); i++) {
                        vertexNewType = vertexNewType.add(addFieldNames.get(i),
                            addFieldProjects.get(i).getType(), true);
                    }
                    vertexName = projectInput.getPathSchema().firstField().get().getName();
                    newPathRecordType = newPathRecordType.addField(vertexName, vertexNewType, true);
                    extendNodeIndex = newPathRecordType.getField(vertexName, true, false).getIndex();
                    extendNodeLabel = newPathRecordType.getFieldList().get(extendNodeIndex).getName();

                    int firstFieldIndex = projectInput.getPathSchema().firstField().get().getIndex();
                    PathInputRef refPathInput = new PathInputRef(vertexName, firstFieldIndex, oldType);
                    PathInputRef leftRex = new PathInputRef(extendNodeLabel, extendNodeIndex,
                        vertexNewType);
                    for (RelDataTypeField field : leftRex.getType().getFieldList()) {
                        VariableInfo variableInfo;
                        RexNode operand;
                        if (addFieldNames.contains(field.getName())) {
                            // cast right expression to field type.
                            int indexOfAddFields = addFieldNames.indexOf(field.getName());
                            operand = builder.getRexBuilder()
                                .makeCast(field.getType(), addFieldProjects.get(indexOfAddFields));
                            variableInfo = new VariableInfo(false, field.getName());
                            rexNodeMap.set(addFieldIndices.get(indexOfAddFields),
                                rexBuilder.makeFieldAccess(leftRex, field.getIndex()));
                        } else {
                            operand = rexBuilder.makeFieldAccess(refPathInput, field.getIndex());
                            variableInfo = new VariableInfo(false, field.getName());
                        }
                        operands.add(operand);
                        rex2VariableInfo.put(operand, variableInfo);
                    }
                    // Construct RexObjectConstruct for dynamic field append expression.
                    RexObjectConstruct rightRex = new RexObjectConstruct(vertexNewType, operands,
                        rex2VariableInfo);
                    List<PathModifyExpression> pathModifyExpressions = new ArrayList<>();
                    pathModifyExpressions.add(new PathModifyExpression(leftRex, rightRex));

                    vertexName = this.generateFieldNames("f", 1,
                        new HashSet<>(newPathRecordType.getFieldNames())).get(0);
                    newPathRecordType = newPathRecordType.addField(vertexName, oldType, true);
                    extendNodeIndex = newPathRecordType.getField(vertexName, true, false).getIndex();
                    PathInputRef leftRex2 = new PathInputRef(vertexName, extendNodeIndex, oldType);
                    Map<RexNode, VariableInfo> vertexRex2VariableInfo = new HashMap<>();
                    List<RexNode> vertexOperands = refPathInput.getType().getFieldList().stream()
                        .map(f -> {
                            RexNode operand = builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, f.getIndex());
                            VariableInfo variableInfo = new VariableInfo(false, f.getName());
                            vertexRex2VariableInfo.put(operand, variableInfo);
                            return operand;
                        }).collect(Collectors.toList());
                    RexObjectConstruct rightRex2 = new RexObjectConstruct(leftRex2.getType(),
                        vertexOperands, vertexRex2VariableInfo);
                    pathModifyExpressions.add(new PathModifyExpression(leftRex2, rightRex2));

                    GQLJavaTypeFactory gqlJavaTypeFactory =
                        (GQLJavaTypeFactory) builder.getTypeFactory();
                    GeaFlowGraph currentGraph = gqlJavaTypeFactory.getCurrentGraph();
                    GraphRecordType graphSchema = (GraphRecordType) currentGraph.getRowType(
                        gqlJavaTypeFactory);
                    return MatchExtend.create(projectInput, pathModifyExpressions,
                        newPathRecordType, graphSchema);
                } else {
                    //Paths start with an edge, add fields on the edge and add an edge extension.
                    edgeNewType = (EdgeRecordType) oldType;
                    addFieldNames = this.generateFieldNames("f", addFieldProjects.size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    for (int i = 0; i < addFieldNames.size(); i++) {
                        edgeNewType = edgeNewType.add(addFieldNames.get(i),
                            addFieldProjects.get(i).getType(), true);
                    }

                    edgeName = projectInput.getPathSchema().firstField().get().getName();
                    newPathRecordType = newPathRecordType.addField(edgeName, edgeNewType, true);
                    extendNodeIndex = newPathRecordType.getField(edgeName, true, false).getIndex();
                    extendNodeLabel = newPathRecordType.getFieldList().get(extendNodeIndex).getName();
                    PathInputRef leftRex = new PathInputRef(extendNodeLabel, extendNodeIndex, edgeNewType);

                    for (RelDataTypeField field : leftRex.getType().getFieldList()) {
                        VariableInfo variableInfo;
                        RexNode operand;
                        if (addFieldNames.contains(field.getName())) {
                            // cast right expression to field type.
                            int indexOfAddFields = addFieldNames.indexOf(field.getName());
                            operand = builder.getRexBuilder().makeCast(field.getType(),
                                addFieldProjects.get(addFieldNames.indexOf(field.getName())));
                            rexNodeMap.set(addFieldIndices.get(indexOfAddFields),
                                rexBuilder.makeFieldAccess(leftRex, field.getIndex()));
                        } else {
                            operand = builder.getRexBuilder()
                                .makeFieldAccess(leftRex, field.getIndex());
                        }
                        variableInfo = new VariableInfo(false, field.getName());
                        operands.add(operand);
                        rex2VariableInfo.put(operand, variableInfo);
                    }
                    // Construct RexObjectConstruct for dynamic field append expression.
                    RexObjectConstruct rightRex = new RexObjectConstruct(leftRex.getType(),
                        operands, rex2VariableInfo);
                    List<PathModifyExpression> pathModifyExpressions = new ArrayList<>();
                    pathModifyExpressions.add(new PathModifyExpression(leftRex, rightRex));

                    edgeName = this.generateFieldNames("f", 1,
                        new HashSet<>(newPathRecordType.getFieldNames())).get(0);
                    newPathRecordType = newPathRecordType.addField(edgeName, oldType, true);
                    extendNodeIndex = newPathRecordType.getField(edgeName, true, false).getIndex();
                    PathInputRef leftRex2 = new PathInputRef(edgeName, extendNodeIndex, oldType);
                    Map<RexNode, VariableInfo> vertexRex2VariableInfo = new HashMap<>();
                    int firstFieldIndex = projectInput.getPathSchema().firstField().get().getIndex();
                    PathInputRef refPathInput = new PathInputRef(edgeName, firstFieldIndex, oldType);
                    List<RexNode> vertexOperands = refPathInput.getType().getFieldList().stream()
                        .map(f -> {
                            RexNode operand = builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, f.getIndex());
                            VariableInfo variableInfo = new VariableInfo(false, f.getName());
                            vertexRex2VariableInfo.put(operand, variableInfo);
                            return operand;
                        }).collect(Collectors.toList());
                    RexObjectConstruct rightRex2 = new RexObjectConstruct(leftRex2.getType(),
                        vertexOperands, vertexRex2VariableInfo);
                    pathModifyExpressions.add(new PathModifyExpression(leftRex2, rightRex2));

                    GQLJavaTypeFactory gqlJavaTypeFactory =
                        (GQLJavaTypeFactory) builder.getTypeFactory();
                    GeaFlowGraph currentGraph = gqlJavaTypeFactory.getCurrentGraph();
                    GraphRecordType graphSchema = (GraphRecordType) currentGraph.getRowType(
                        gqlJavaTypeFactory);
                    return MatchExtend.create(projectInput, pathModifyExpressions,
                        newPathRecordType, graphSchema);
                }
            } else {
                return projectInput;
            }
        } else if (from instanceof LogicalAggregate) {
            LogicalAggregate aggregate = (LogicalAggregate) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode aggregateInput = from == to ? input : concatToMatchNode(builder, left,
                GQLRelUtil.toRel(aggregate.getInput()), to, input, inputRexNode2RexInfo);
            int lastNodeIndex = aggregateInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            //MatchAggregate needs to reference path fields, not using indices for referencing,
            // but using RexNode instead.
            List<MatchAggregateCall> adjustAggCalls;
            List<RexNode> adjustGroupList;
            Set<String> matchAggPathLabels = new HashSet<>();
            if (!inputRexNode2RexInfo.isEmpty()) {
                adjustGroupList = aggregate.getGroupSet().asList().stream()
                    .map(inputRexNode2RexInfo::get).collect(Collectors.toList());
                adjustAggCalls = aggregate.getAggCallList().stream().map(
                    aggCall -> {
                        List<RexNode> newArgList = aggCall.getArgList().stream()
                            .map(inputRexNode2RexInfo::get).collect(Collectors.toList());
                        return new MatchAggregateCall(aggCall.getAggregation(), aggCall.isDistinct(),
                            aggCall.isApproximate(), newArgList, aggCall.filterArg,
                            aggCall.getCollation(), aggCall.getType(), aggCall.getName());
                    }).collect(Collectors.toList());
            } else {
                String lastNodeLabel = aggregateInput.getPathSchema().getFieldList()
                    .get(lastNodeIndex).getName();
                matchAggPathLabels.add(lastNodeLabel);
                RelDataType oldType = aggregateInput.getPathSchema().getFieldList()
                    .get(lastNodeIndex).getType();
                adjustGroupList = aggregate.getGroupSet().asList().stream().map(idx -> builder.getRexBuilder()
                    .makeFieldAccess(new PathInputRef(lastNodeLabel, lastNodeIndex, oldType),
                        idx)).collect(Collectors.toList());
                adjustAggCalls = aggregate.getAggCallList().stream().map(
                    aggCall -> new MatchAggregateCall(aggCall.getAggregation(),
                        aggCall.isDistinct(), aggCall.isApproximate(),
                        aggCall.getArgList().stream().map(idx -> builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, oldType), idx)).collect(Collectors.toList()), aggCall.filterArg, aggCall.getCollation(),
                        aggCall.getType(), aggCall.getName())).collect(Collectors.toList());
            }

            //Get the pruning path.
            if (left != null) {
                matchAggPathLabels.addAll(left.getPathSchema().getFieldNames());
            }
            for (RelDataTypeField field : aggregateInput.getPathSchema().getFieldList()) {
                if (field.getType() instanceof VertexRecordType && adjustGroupList.stream()
                    .anyMatch(rexNode -> ((PathInputRef) ((RexFieldAccess) rexNode)
                        .getReferenceExpr()).getLabel().equals(field.getName())
                        && rexNode.getType() instanceof MetaFieldType
                        && ((MetaFieldType) rexNode.getType()).getMetaField()
                        .equals(MetaField.VERTEX_ID))) {
                    //The condition for preserving vertex in the after aggregating path is that the
                    // vertex ID appears in the Group.
                    matchAggPathLabels.add(field.getName());
                } else if (field.getType() instanceof EdgeRecordType) {
                    //The condition for preserving edges in the after aggregating path is that
                    // the edge srcId, targetId, and timestamp appear in the Group.
                    //todo: Since the GQL inferred from SQL does not have a Union, we will
                    // temporarily not consider Labels here.
                    boolean groupBySrcId = adjustGroupList.stream().anyMatch(rexNode ->
                        ((PathInputRef) ((RexFieldAccess) rexNode)
                            .getReferenceExpr()).getLabel().equals(field.getName())
                            && rexNode.getType() instanceof MetaFieldType
                            && ((MetaFieldType) rexNode.getType()).getMetaField()
                            .equals(MetaField.EDGE_SRC_ID));
                    boolean groupByTargetId = adjustGroupList.stream().anyMatch(rexNode ->
                        ((PathInputRef) ((RexFieldAccess) rexNode)
                            .getReferenceExpr()).getLabel().equals(field.getName())
                            && rexNode.getType() instanceof MetaFieldType
                            && ((MetaFieldType) rexNode.getType()).getMetaField()
                            .equals(MetaField.EDGE_TARGET_ID));
                    boolean groupByTs = true;
                    if (((EdgeRecordType) field.getType()).getTimestampField().isPresent()) {
                        groupByTs = adjustGroupList.stream().anyMatch(rexNode ->
                            ((PathInputRef) ((RexFieldAccess) rexNode)
                                .getReferenceExpr()).getLabel().equals(field.getName())
                                && rexNode.getType() instanceof MetaFieldType
                                && ((MetaFieldType) rexNode.getType()).getMetaField()
                                .equals(MetaField.EDGE_TS));
                    }
                    if (groupBySrcId && groupByTargetId && groupByTs) {
                        matchAggPathLabels.add(field.getName());
                    }
                }
            }
            if (matchAggPathLabels.isEmpty()) {
                matchAggPathLabels.add(aggregateInput.getPathSchema().firstFieldName().get());
            }

            PathRecordType aggPathType;
            if (adjustGroupList.size() > 0 || aggregate.getAggCallList().size() > 0) {
                PathRecordType pathType = aggregateInput.getPathSchema();
                PathRecordType prunePathType = new PathRecordType(pathType.getFieldList().stream()
                    .filter(f -> matchAggPathLabels.contains(f.getName()))
                    .collect(Collectors.toList()));
                //Prune the path, and add the aggregated value to the beginning of the path.
                RelDataType firstNodeType = prunePathType.firstField().get().getType();
                String firstNodeName = prunePathType.firstField().get().getName();
                int offset;
                if (firstNodeType instanceof VertexRecordType) {
                    VertexRecordType vertexNewType = (VertexRecordType) firstNodeType;
                    List<String> addFieldNames = this.generateFieldNames("f", adjustGroupList.size(),
                        new HashSet<>(vertexNewType.getFieldNames()));
                    offset = vertexNewType.getFieldCount();
                    for (int i = 0; i < adjustGroupList.size(); i++) {
                        RelDataType dataType = adjustGroupList.get(i).getType();
                        vertexNewType = vertexNewType.add(addFieldNames.get(i), dataType, true);
                    }
                    addFieldNames = generateFieldNames("agg", aggregate.getAggCallList().size(),
                        new HashSet<>(vertexNewType.getFieldNames()));
                    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                        vertexNewType = vertexNewType.add(addFieldNames.get(i),
                            aggregate.getAggCallList().get(i).getType(), true);
                    }
                    for (int i = 0; i < adjustGroupList.size() + aggregate.getAggCallList().size(); i++) {
                        rexNodeMap.add(builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(firstNodeName, 0, vertexNewType), offset + i));
                    }
                    aggPathType = new PathRecordType(new ArrayList<>())
                        .addField(firstNodeName, vertexNewType, true);
                    aggPathType = aggPathType.concat(prunePathType, true);
                } else if (firstNodeType instanceof EdgeRecordType) {
                    EdgeRecordType edgeNewType = (EdgeRecordType) firstNodeType;
                    List<String> addFieldNames = this.generateFieldNames("f", adjustGroupList.size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    offset = edgeNewType.getFieldCount();
                    for (int i = 0; i < adjustGroupList.size(); i++) {
                        RelDataType dataType = adjustGroupList.get(i).getType();
                        edgeNewType = edgeNewType.add(addFieldNames.get(i), dataType, true);
                    }
                    addFieldNames = generateFieldNames("agg", aggregate.getAggCallList().size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                        edgeNewType = edgeNewType.add(addFieldNames.get(i),
                            aggregate.getAggCallList().get(i).getType(), true);
                    }
                    for (int i = 0; i < adjustGroupList.size() + aggregate.getAggCallList().size(); i++) {
                        rexNodeMap.add(builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(firstNodeName, 0, edgeNewType), offset + i));
                    }
                    aggPathType = new PathRecordType(new ArrayList<>())
                        .addField(firstNodeName, edgeNewType, true);
                    aggPathType = aggPathType.concat(prunePathType, true);
                } else {
                    throw new GeaFlowDSLException("Path node should be vertex or edge.");
                }
                return MatchAggregate.create(aggregateInput, aggregate.indicator, adjustGroupList,
                    adjustAggCalls, aggPathType);
            } else {
                return aggregateInput;
            }
        }
        return input;
    }

    /**
     * Generate n non-repeating field names with the format "[$prefix][$index_number]".
     */
    protected List<String> generateFieldNames(String prefix, int nameCount,
                                              Collection<String> existsNames) {
        if (nameCount <= 0) {
            return Collections.emptyList();
        }
        Set<String> exists = new HashSet<>(existsNames);
        List<String> validNames = new ArrayList<>(nameCount);
        int i = 0;
        while (validNames.size() < nameCount) {
            String newName = prefix + i;
            if (!exists.contains(newName)) {
                validNames.add(newName);
            }
            i++;
        }
        return validNames;
    }

    /**
     * Replace the references to path nodes in the leftRexNodes with references to
     * the newGraphMatch directly, based on the label of the reference.
     */
    protected List<RexNode> adjustLeftRexNodes(List<RexNode> leftRexNodes, GraphMatch newGraphMatch,
                                               RelBuilder builder) {
        return leftRexNodes.stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
            if (rexNode instanceof RexFieldAccess
                && ((RexFieldAccess) rexNode).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) rexNode).getReferenceExpr();
                String label = pathInputRef.getLabel();
                int index = newGraphMatch.getRowType().getField(label, true, false).getIndex();
                RelDataType type = newGraphMatch.getRowType().getField(label, true, false)
                    .getType();
                PathInputRef vertexRef = new PathInputRef(label, index, type);
                return builder.getRexBuilder().makeFieldAccess(vertexRef, ((RexFieldAccess) rexNode).getField().getIndex());

            }
            return rexNode;
        })).collect(Collectors.toList());
    }

    /**
     * Replace the references to path nodes in the leftRexNodes with references to
     * the newGraphMatch, based on the label of the reference. If the label of the reference
     * exists in the left branch, automatically add an offset to make it a reference to the same
     * name Node in the right branch.
     */
    protected List<RexNode> adjustRightRexNodes(List<RexNode> rightRexNodes,
                                                GraphMatch newGraphMatch, RelBuilder builder,
                                                IMatchNode leftPathPattern,
                                                IMatchNode rightPathPattern) {
        final IMatchNode finalLeft = leftPathPattern;
        final IMatchNode finalRight = rightPathPattern;
        return rightRexNodes.stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
            if (rexNode instanceof RexFieldAccess
                && ((RexFieldAccess) rexNode).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) rexNode).getReferenceExpr();
                String label = pathInputRef.getLabel();
                boolean isConflictLabel = leftPathPattern.getPathSchema().getFieldNames()
                    .contains(label);
                if (isConflictLabel) {
                    int index = finalRight.getRowType().getField(label, true, false).getIndex();
                    index += finalLeft.getRowType().getFieldCount();
                    label = newGraphMatch.getRowType().getFieldList().get(index).getName();
                    RelDataType type = newGraphMatch.getRowType().getFieldList().get(index)
                        .getType();
                    PathInputRef vertexRef = new PathInputRef(label, index, type);
                    return builder.getRexBuilder().makeFieldAccess(vertexRef,
                        ((RexFieldAccess) rexNode).getField().getIndex());
                } else {
                    int index = newGraphMatch.getRowType().getField(label, true, false).getIndex();
                    RelDataType type = newGraphMatch.getRowType().getFieldList().get(index)
                        .getType();
                    PathInputRef vertexRef = new PathInputRef(label, index, type);
                    return builder.getRexBuilder().makeFieldAccess(vertexRef,
                        ((RexFieldAccess) rexNode).getField().getIndex());
                }
            }
            return rexNode;
        })).collect(Collectors.toList());
    }

    /**
     * Handling the transformation of SQL RelNodes for the MatchJoinTableToGraphMatch
     * and TableJoinMatchToGraphMatch rules into GQL matches.
     */
    protected RelNode processGraphMatchJoinTable(RelOptRuleCall call, LogicalJoin join,
                                                 LogicalGraphMatch graphMatch,
                                                 LogicalProject project,
                                                 LogicalTableScan tableScan,
                                                 RelNode leftInput, RelNode leftHead,
                                                 RelNode rightInput, RelNode rightHead,
                                                 boolean isMatchInLeft) {
        GeaFlowTable geaflowTable = tableScan.getTable().unwrap(GeaFlowTable.class);
        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph currentGraph = typeFactory.getCurrentGraph();
        if (!currentGraph.containTable(geaflowTable)) {
            if (geaflowTable instanceof VertexTable || geaflowTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    geaflowTable.getName());
            }
            return null;
        }
        GraphJoinType graphJoinType = getJoinType(join);
        RelDataType tableType = tableScan.getRowType();
        IMatchNode matchNode = graphMatch.getPathPattern();
        RelBuilder relBuilder = call.builder();
        List<RexNode> rexLeftNodeMap = new ArrayList<>();
        List<RexNode> rexRightNodeMap = new ArrayList<>();
        IMatchNode concatedLeftMatch;
        IMatchNode newPathPattern;
        boolean isEdgeReverse = false;
        switch (graphJoinType) {
            case EDGE_JOIN_VERTEX:
            case VERTEX_JOIN_EDGE:
                if (geaflowTable instanceof VertexTable) { // graph match join vertex table
                    if (matchNode instanceof SingleMatchNode && GQLRelUtil.getLatestMatchNode(
                        (SingleMatchNode) matchNode) instanceof EdgeMatch) {
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, leftInput,
                                leftHead, matchNode, rexLeftNodeMap)
                                : concatToMatchNode(relBuilder, null, rightInput,
                                rightHead, matchNode, rexRightNodeMap);
                        String nodeName = geaflowTable.getName();
                        PathRecordType pathRecordType =
                            concatedLeftMatch.getPathSchema().getFieldNames().contains(nodeName)
                                ? concatedLeftMatch.getPathSchema()
                                : concatedLeftMatch.getPathSchema().addField(nodeName, tableType, false);
                        switch (join.getJoinType()) {
                            case LEFT:
                                newPathPattern = OptionalVertexMatch.create(concatedLeftMatch.getCluster(),
                                    (SingleMatchNode) concatedLeftMatch, nodeName,
                                    Collections.singletonList(nodeName), tableType, pathRecordType);
                                break;
                            case INNER:
                                newPathPattern = VertexMatch.create(concatedLeftMatch.getCluster(),
                                    (SingleMatchNode) concatedLeftMatch, nodeName,
                                    Collections.singletonList(nodeName), tableType, pathRecordType);
                                break;
                            case RIGHT:
                            case FULL:
                            default:
                                throw new GeaFlowDSLException("Illegal join type: {}", join.getJoinType());
                        }
                        newPathPattern =
                            isMatchInLeft ? concatToMatchNode(relBuilder, concatedLeftMatch, rightInput,
                                rightHead, newPathPattern, rexRightNodeMap)
                                : concatToMatchNode(relBuilder, concatedLeftMatch, leftInput,
                                leftHead, newPathPattern, rexLeftNodeMap);
                    } else {
                        String nodeName = geaflowTable.getName();
                        assert currentGraph.getVertexTables().stream()
                            .anyMatch(t -> t.getName().equalsIgnoreCase(geaflowTable.getName()));
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                : concatToMatchNode(relBuilder, null, rightInput, rightHead,
                                matchNode, rexRightNodeMap);
                        RelDataType vertexRelType = geaflowTable.getRowType(
                            relBuilder.getTypeFactory());
                        PathRecordType rightPathType = PathRecordType.EMPTY.addField(geaflowTable.getName(),
                            vertexRelType, false);
                        VertexMatch rightVertexMatch = VertexMatch.create(concatedLeftMatch.getCluster(), null,
                            nodeName, Collections.singletonList(geaflowTable.getName()),
                            vertexRelType, rightPathType);
                        IMatchNode matchJoinRight =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, rightInput,
                                rightHead, rightVertexMatch, rexRightNodeMap)
                                : concatToMatchNode(relBuilder, null, leftInput,
                                leftHead, rightVertexMatch, rexLeftNodeMap);
                        MatchJoin matchJoin = MatchJoin.create(concatedLeftMatch.getCluster(),
                            concatedLeftMatch.getTraitSet(), concatedLeftMatch, matchJoinRight,
                            relBuilder.getRexBuilder().makeLiteral(true), join.getJoinType());

                        PathInputRef vertexRef = new PathInputRef(nodeName,
                            matchJoin.getRowType().getField(nodeName, true, false).getIndex(),
                            matchJoin.getRowType().getField(nodeName, true, false).getType());
                        RexNode operand1 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        RelDataTypeField field = matchJoin.getRowType().getFieldList().get(
                            matchJoin.getRowType().getFieldCount() - rightVertexMatch.getRowType()
                                .getFieldCount());
                        vertexRef = new PathInputRef(field.getName(), field.getIndex(),
                            field.getType());
                        RexNode operand2 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        SqlOperator equalsOperator = SqlStdOperatorTable.EQUALS;
                        RexNode condition = relBuilder.getRexBuilder()
                            .makeCall(equalsOperator, operand1, operand2);
                        newPathPattern = matchJoin.copy(matchJoin.getTraitSet(), condition,
                            matchJoin.getLeft(), matchJoin.getRight(), matchJoin.getJoinType());
                    }
                } else if (geaflowTable instanceof EdgeTable) {
                    if (matchNode instanceof SingleMatchNode && GQLRelUtil.getLatestMatchNode(
                        (SingleMatchNode) matchNode) instanceof VertexMatch) {
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                : concatToMatchNode(relBuilder, null, rightInput, rightHead,
                                matchNode, rexRightNodeMap);
                        isEdgeReverse = graphJoinType.equals(GraphJoinType.EDGE_JOIN_VERTEX);
                        EdgeDirection edgeDirection =
                            isEdgeReverse ? EdgeDirection.IN : EdgeDirection.OUT;
                        String edgeName = geaflowTable.getName();
                        PathRecordType pathRecordType =
                            concatedLeftMatch.getPathSchema().getFieldNames().contains(edgeName)
                                ? concatedLeftMatch.getPathSchema()
                                : concatedLeftMatch.getPathSchema().addField(edgeName, tableType, false);
                        switch (join.getJoinType()) {
                            case LEFT:
                                if (!isMatchInLeft) {
                                    LOGGER.warn("Left table cannot be forcibly retained. Use INNER Join instead.");
                                }
                                newPathPattern = OptionalEdgeMatch.create(concatedLeftMatch.getCluster(),
                                    (SingleMatchNode) concatedLeftMatch, edgeName,
                                    Collections.singletonList(edgeName), edgeDirection, tableType,
                                    pathRecordType);
                                break;
                            case INNER:
                                newPathPattern = EdgeMatch.create(concatedLeftMatch.getCluster(),
                                    (SingleMatchNode) concatedLeftMatch, edgeName,
                                    Collections.singletonList(edgeName), edgeDirection, tableType,
                                    pathRecordType);
                                break;
                            case RIGHT:
                            case FULL:
                            default:
                                throw new GeaFlowDSLException("Illegal join type: {}", join.getJoinType());
                        }
                        newPathPattern =
                            isMatchInLeft ? concatToMatchNode(relBuilder, concatedLeftMatch, rightInput,
                                rightHead, newPathPattern, rexRightNodeMap)
                                : concatToMatchNode(relBuilder, concatedLeftMatch, leftInput,
                                leftHead, newPathPattern, rexLeftNodeMap);

                    } else {
                        String edgeName = geaflowTable.getName();
                        GraphDescriptor graphDescriptor = currentGraph.getDescriptor();
                        Optional<EdgeDescriptor> edgeDesc = graphDescriptor.edges.stream()
                            .filter(e -> e.type.equals(geaflowTable.getName())).findFirst();
                        VertexTable dummyVertex = null;
                        if (edgeDesc.isPresent()) {
                            EdgeDescriptor edgeDescriptor = edgeDesc.get();
                            String dummyNodeType = edgeDescriptor.sourceType;
                            dummyVertex = currentGraph.getVertexTables().stream()
                                .filter(v -> v.getName().equals(dummyNodeType)).findFirst().get();
                        }
                        if (dummyVertex == null) {
                            return null;
                        }
                        String dummyNodeName = dummyVertex.getName();
                        RelDataType dummyVertexRelType = dummyVertex.getRowType(
                            relBuilder.getTypeFactory());
                        PathRecordType pathRecordType = new PathRecordType(
                            new ArrayList<>()).addField(dummyNodeName, dummyVertexRelType, true);
                        VertexMatch dummyVertexMatch = VertexMatch.create(matchNode.getCluster(),
                            null, dummyNodeName, Collections.singletonList(dummyVertex.getName()),
                            dummyVertexRelType, pathRecordType);
                        RelDataType edgeRelType = geaflowTable.getRowType(
                            relBuilder.getTypeFactory());
                        pathRecordType = pathRecordType.addField(edgeName, edgeRelType, true);
                        EdgeMatch edgeMatch = EdgeMatch.create(matchNode.getCluster(),
                            dummyVertexMatch, edgeName,
                            Collections.singletonList(geaflowTable.getName()), EdgeDirection.OUT,
                            edgeRelType, pathRecordType);

                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                : concatToMatchNode(relBuilder, null, rightInput, rightHead,
                                matchNode, rexRightNodeMap);
                        IMatchNode matchJoinRight =
                            isMatchInLeft ? concatToMatchNode(relBuilder, null, rightInput,
                                rightHead, edgeMatch, rexRightNodeMap)
                                : concatToMatchNode(relBuilder, null, leftInput,
                                leftHead, edgeMatch, rexLeftNodeMap);
                        MatchJoin matchJoin = MatchJoin.create(matchNode.getCluster(),
                            matchNode.getTraitSet(), concatedLeftMatch, matchJoinRight,
                            relBuilder.getRexBuilder().makeLiteral(true), join.getJoinType());

                        PathInputRef vertexRef = new PathInputRef(dummyNodeName,
                            matchJoin.getRowType().getField(dummyNodeName, true, false).getIndex(),
                            matchJoin.getRowType().getField(dummyNodeName, true, false).getType());
                        RexNode operand1 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        RelDataTypeField field = matchJoin.getRowType().getFieldList().get(
                            matchJoin.getRowType().getFieldCount() - edgeMatch.getRowType()
                                .getFieldCount());
                        vertexRef = new PathInputRef(field.getName(), field.getIndex(),
                            field.getType());
                        RexNode operand2 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        SqlOperator equalsOperator = SqlStdOperatorTable.EQUALS;
                        RexNode condition = relBuilder.getRexBuilder()
                            .makeCall(equalsOperator, operand1, operand2);
                        newPathPattern = matchJoin.copy(matchJoin.getTraitSet(), condition,
                            matchJoin.getLeft(), matchJoin.getRight(), matchJoin.getJoinType());
                    }
                } else {
                    return null;
                }
                break;
            default:
                return null;
        }
        if (newPathPattern == null) {
            return null;
        }
        GraphMatch newGraphMatch = graphMatch.copy(newPathPattern);
        // Add the original Projects from the GraphMatch branch, filtering out fields that
        // no longer exist after rebuilding GraphMatch inputs.
        List<RexNode> oldProjects = project.getProjects().stream().filter(prj -> {
            if (prj instanceof RexFieldAccess
                && ((RexFieldAccess) prj).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) prj).getReferenceExpr();
                String label = pathInputRef.getLabel();
                RelDataTypeField pathField = newGraphMatch.getRowType().getField(label, true, false);
                if (pathField != null) {
                    RexFieldAccess fieldAccess = (RexFieldAccess) prj;
                    int index = fieldAccess.getField().getIndex();
                    return index < pathField.getType().getFieldList().size()
                        && fieldAccess.getField().equals(pathField.getType().getFieldList().get(index));
                }
            }
            return false;
        }).collect(Collectors.toList());
        List<RexNode> newProjects = new ArrayList<>();
        if (newPathPattern instanceof MatchJoin) {
            newProjects.addAll(
                isMatchInLeft ? adjustLeftRexNodes(oldProjects, newGraphMatch,
                    relBuilder) : adjustRightRexNodes(oldProjects, newGraphMatch,
                    relBuilder, (IMatchNode) newPathPattern.getInput(0),
                    (IMatchNode) newPathPattern.getInput(1)));
        } else {
            newProjects.addAll(adjustLeftRexNodes(oldProjects, newGraphMatch, relBuilder));
        }

        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        // Add fields of the Table into the projects.
        String tableName = geaflowTable.getName();
        RelDataTypeField pathTableField = newPathPattern.getPathSchema()
            .getField(tableName, true, false);
        List<RexNode> tableProjects = new ArrayList<>();
        if (pathTableField != null) {
            int baseOffset = newProjects.size();
            PathInputRef pathTableRef = new PathInputRef(tableName, pathTableField.getIndex(),
                pathTableField.getType());
            tableProjects = tableType.getFieldList().stream()
                .map(f -> rexBuilder.makeFieldAccess(pathTableRef, f.getIndex()))
                .collect(Collectors.toList());
            newProjects.addAll(tableProjects);

            //In the case of reverse matching in the IN direction, the positions of the source
            // vertex and the destination vertex are swapped.
            if (isEdgeReverse) {
                int edgeSrcIdIndex = tableType.getFieldList().stream().filter(
                    f -> f.getType() instanceof MetaFieldType
                            && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
                    .collect(Collectors.toList()).get(0).getIndex();
                int edgeTargetIdIndex = tableType.getFieldList().stream().filter(
                    f -> f.getType() instanceof MetaFieldType
                            && ((MetaFieldType) f.getType()).getMetaField()
                            .equals(MetaField.EDGE_TARGET_ID)).collect(Collectors.toList()).get(0)
                    .getIndex();
                Collections.swap(newProjects, baseOffset + edgeSrcIdIndex,
                    baseOffset + edgeTargetIdIndex);
            }
        }

        // Add fields newly added in the rebuild of the left branch.
        if (rexLeftNodeMap.size() > 0) {
            List<RexNode> tmpLeftProjects = new ArrayList<>(rexLeftNodeMap);
            newProjects.addAll(adjustLeftRexNodes(tmpLeftProjects, newGraphMatch, relBuilder));
        }

        // Add fields newly added in the rebuild of the right branch.
        if (rexRightNodeMap.size() > 0) {
            List<RexNode> tmpRightProjects = new ArrayList<>(rexRightNodeMap);
            if (newPathPattern instanceof MatchJoin) {
                newProjects.addAll(adjustRightRexNodes(tmpRightProjects, newGraphMatch, relBuilder,
                    (IMatchNode) newPathPattern.getInput(0),
                    (IMatchNode) newPathPattern.getInput(1)));
            } else {
                newProjects.addAll(adjustLeftRexNodes(tmpRightProjects, newGraphMatch, relBuilder));
            }
        }

        // Complete the projection from Path to Row.
        List<RelDataTypeField> matchTypeFields = new ArrayList<>();
        List<String> newFieldNames = this.generateFieldNames("f", newProjects.size(), new HashSet<>());
        for (int i = 0; i < newProjects.size(); i++) {
            matchTypeFields.add(
                new RelDataTypeFieldImpl(newFieldNames.get(i), i, newProjects.get(i).getType()));
        }
        RelNode tail = LogicalProject.create(newGraphMatch, newProjects,
            new RelRecordType(matchTypeFields));

        // Complete the Join projection.
        if (newPathPattern instanceof MatchJoin) {
            rexRightNodeMap = adjustRightRexNodes(rexRightNodeMap, newGraphMatch, relBuilder,
                (IMatchNode) newPathPattern.getInput(0), (IMatchNode) newPathPattern.getInput(1));
        }
        List<RexNode> joinProjects = new ArrayList<>();
        //  If the left branch undergoes rebuilding, take the reconstructed Rex from
        //  the left branch, otherwise take the original Projects.
        final RelNode finalTail = tail;
        int projectFieldCount = oldProjects.size();
        int joinFieldCount = projectFieldCount + tableProjects.size();
        if (rexLeftNodeMap.size() > 0) {
            joinProjects.addAll(
                IntStream.range(joinFieldCount, joinFieldCount + rexLeftNodeMap.size())
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
        } else {
            if (isMatchInLeft) {
                joinProjects.addAll(IntStream.range(0, projectFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            } else {
                joinProjects.addAll(IntStream.range(projectFieldCount, joinFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            }
        }
        // If the right branch undergoes rebuilding, take the reconstructed Rex from the right
        // branch, otherwise take the original Projects.
        if (rexRightNodeMap.size() > 0) {
            joinProjects.addAll(IntStream.range(joinFieldCount + rexLeftNodeMap.size(),
                    joinFieldCount + rexLeftNodeMap.size() + rexRightNodeMap.size())
                .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i)).collect(Collectors.toList()));
        } else {
            if (isMatchInLeft) {
                joinProjects.addAll(IntStream.range(projectFieldCount, joinFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            } else {
                joinProjects.addAll(IntStream.range(0, projectFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            }
        }
        AtomicInteger offset = new AtomicInteger();
        // Make the project type nullable the same as the output type of the join.
        joinProjects = joinProjects.stream().map(prj -> {
            int i = offset.getAndIncrement();
            boolean joinFieldNullable = join.getRowType().getFieldList().get(i).getType().isNullable();
            if ((prj.getType().isNullable() && !joinFieldNullable)
                || (!prj.getType().isNullable() && joinFieldNullable)) {
                RelDataType type = rexBuilder.getTypeFactory().createTypeWithNullability(prj.getType(), joinFieldNullable);
                return rexBuilder.makeCast(type, prj);
            }
            return prj;
        }).collect(Collectors.toList());
        tail = LogicalProject.create(tail, joinProjects, join.getRowType());
        return tail;
    }

    public enum GraphJoinType {
        /**
         * Vertex join edge src id.
         */
        VERTEX_JOIN_EDGE,
        /**
         * Edge target id join vertex.
         */
        EDGE_JOIN_VERTEX,
        /**
         * None graph match type.
         */
        NONE_GRAPH_JOIN
    }
}
