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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphScan;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.OptionalEdgeMatch;
import org.apache.geaflow.dsl.rel.match.OptionalVertexMatch;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableJoinTableToGraphRule extends AbstractJoinToGraphRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableJoinTableToGraphRule.class);

    public static final TableJoinTableToGraphRule INSTANCE = new TableJoinTableToGraphRule();

    private TableJoinTableToGraphRule() {
        super(operand(LogicalJoin.class,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        if (!isSupportJoinType(join.getJoinType())) {
            // non-INNER joins is not supported.
            return false;
        }
        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);
        return isSingleChainFromLogicalTableScan(leftInput) && isSingleChainFromLogicalTableScan(rightInput);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode leftInput = call.rel(1);
        RelNode leftHead = null;
        RelNode leftTableScan = leftInput;
        while (!(leftTableScan instanceof LogicalTableScan)) {
            leftHead = leftTableScan;
            leftTableScan = GQLRelUtil.toRel(leftTableScan.getInput(0));
        }
        RelNode rightInput = call.rel(2);
        RelNode rightHead = null;
        RelNode rightTableScan = rightInput;
        while (!(rightTableScan instanceof LogicalTableScan)) {
            rightHead = rightTableScan;
            rightTableScan = GQLRelUtil.toRel(rightTableScan.getInput(0));
        }
        GeaFlowTable leftTable = leftTableScan.getTable().unwrap(GeaFlowTable.class);
        GeaFlowTable rightTable = rightTableScan.getTable().unwrap(GeaFlowTable.class);

        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph currentGraph = typeFactory.getCurrentGraph();
        if (!currentGraph.containTable(leftTable)) {
            if (leftTable instanceof VertexTable || leftTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    leftTable.getName());
            }
            return;
        }
        if (!currentGraph.containTable(rightTable)) {
            if (rightTable instanceof VertexTable || rightTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    rightTable.getName());
            }
            return;
        }

        LogicalJoin join = call.rel(0);
        GraphJoinType graphJoinType = getJoinType(join);
        RelNode tail = null;
        switch (graphJoinType) {
            case VERTEX_JOIN_EDGE:
                if (leftTable instanceof VertexTable
                    && rightTable instanceof EdgeTable) {
                    VertexTable vertexTable = (VertexTable) leftTable;
                    EdgeTable edgeTable = (EdgeTable) rightTable;
                    tail = vertexJoinEdgeSrc(vertexTable, edgeTable, call, true,
                        leftInput, leftHead, rightInput, rightHead);
                } else if (leftTable instanceof EdgeTable
                    && rightTable instanceof VertexTable) {
                    VertexTable vertexTable = (VertexTable) rightTable;
                    EdgeTable edgeTable = (EdgeTable) leftTable;
                    tail = vertexJoinEdgeSrc(vertexTable, edgeTable, call, false,
                        leftInput, leftHead, rightInput, rightHead);
                }
                if (tail == null) {
                    return;
                }
                break;
            case EDGE_JOIN_VERTEX:
                if (leftTable instanceof VertexTable
                    && rightTable instanceof EdgeTable) {
                    VertexTable vertexTable = (VertexTable) leftTable;
                    EdgeTable edgeTable = (EdgeTable) rightTable;
                    tail = edgeTargetJoinVertex(vertexTable, edgeTable, call, true,
                        leftInput, leftHead, rightInput, rightHead);
                } else if (leftTable instanceof EdgeTable
                    && rightTable instanceof VertexTable) {
                    VertexTable vertexTable = (VertexTable) rightTable;
                    EdgeTable edgeTable = (EdgeTable) leftTable;
                    tail = edgeTargetJoinVertex(vertexTable, edgeTable, call, false,
                        leftInput, leftHead, rightInput, rightHead);
                }
                if (tail == null) {
                    return;
                }
                break;
            default:
        }
        if (tail == null) {
            return;
        }
        // add remain filter.
        JoinInfo joinInfo = join.analyzeCondition();
        RexNode remainFilter = joinInfo.getRemaining(join.getCluster().getRexBuilder());
        if (remainFilter != null && !remainFilter.isAlwaysTrue()) {
            tail = LogicalFilter.create(tail, remainFilter);
        }
        call.transformTo(tail);
    }

    private RelNode vertexJoinEdgeSrc(VertexTable vertexTable,
                                      EdgeTable edgeTable,
                                      RelOptRuleCall call,
                                      boolean isLeftTableVertex,
                                      RelNode leftInput, RelNode leftHead,
                                      RelNode rightInput, RelNode rightHead) {
        return vertexJoinEdge(vertexTable, edgeTable, call, EdgeDirection.OUT, isLeftTableVertex,
            leftInput, leftHead, rightInput, rightHead);
    }

    private RelNode edgeTargetJoinVertex(VertexTable vertexTable,
                                         EdgeTable edgeTable,
                                         RelOptRuleCall call,
                                         boolean isLeftTableVertex,
                                         RelNode leftInput, RelNode leftHead,
                                         RelNode rightInput, RelNode rightHead) {
        return vertexJoinEdge(vertexTable, edgeTable, call, EdgeDirection.IN, isLeftTableVertex,
            leftInput, leftHead, rightInput, rightHead);
    }

    private RelNode vertexJoinEdge(VertexTable vertexTable,
                                   EdgeTable edgeTable,
                                   RelOptRuleCall call,
                                   EdgeDirection direction,
                                   boolean isLeftTableVertex,
                                   RelNode leftInput, RelNode leftHead,
                                   RelNode rightInput, RelNode rightHead) {
        LogicalJoin join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        RelDataType vertexRelType = vertexTable.getRowType(call.builder().getTypeFactory());
        PathRecordType pathRecordType = PathRecordType.EMPTY;
        String nodeName = vertexTable.getName();
        RelDataType edgeRelType = edgeTable.getRowType(call.builder().getTypeFactory());
        String edgeName = edgeTable.getName();

        VertexMatch vertexMatch;
        EdgeMatch edgeMatch;
        IMatchNode matchNode;
        boolean swapSrcTargetId;
        List<RexNode> projects = new ArrayList<>();
        int leftFieldCount;
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        List<RexNode> rexLeftNodeMap = new ArrayList<>();
        List<RexNode> rexRightNodeMap = new ArrayList<>();
        if (isLeftTableVertex) {
            pathRecordType = pathRecordType.addField(nodeName, vertexRelType, false);
            vertexMatch = VertexMatch.create(cluster, null, nodeName,
                Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            IMatchNode afterLeft = concatToMatchNode(call.builder(), null, leftInput, leftHead,
                vertexMatch, rexLeftNodeMap);
            //Add vertex fields
            if (rexLeftNodeMap.size() > 0) {
                projects.addAll(rexLeftNodeMap);
            } else {
                RelDataTypeField field = afterLeft.getPathSchema().getField(nodeName, true, false);
                PathInputRef vertexRef = new PathInputRef(nodeName, field.getIndex(), field.getType());
                for (int i = 0; i < leftInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(vertexRef, i));
                }
            }
            leftFieldCount = projects.size();

            pathRecordType = afterLeft.getPathSchema();
            pathRecordType = pathRecordType.addField(edgeName, edgeRelType, true);
            //When joining vertex and edge in a LEFT JOIN, the vertex is forcibly retained.
            switch (join.getJoinType()) {
                case LEFT:
                    edgeMatch = OptionalEdgeMatch.create(cluster, (SingleMatchNode) afterLeft, edgeName,
                        Collections.singletonList(edgeTable.getName()), direction, edgeRelType,
                        pathRecordType);
                    break;
                case INNER:
                    edgeMatch = EdgeMatch.create(cluster, (SingleMatchNode) afterLeft, edgeName,
                        Collections.singletonList(edgeTable.getName()), direction, edgeRelType,
                        pathRecordType);
                    break;
                case RIGHT:
                case FULL:
                default:
                    throw new GeaFlowDSLException("Illegal join type: {}", join.getJoinType());
            }
            swapSrcTargetId = direction.equals(EdgeDirection.IN);
            IMatchNode afterRight = concatToMatchNode(call.builder(), afterLeft,
                rightInput, rightHead, edgeMatch, rexRightNodeMap);
            //Add edge fields
            if (rexRightNodeMap.size() > 0) {
                // In the case of converting match out edges to in edges, swap the source and
                // target id references of the edge.
                if (swapSrcTargetId) {
                    rexRightNodeMap = rexRightNodeMap.stream()
                        .map(rex -> GQLRexUtil.swapReverseEdgeRef(rex, edgeName, rexBuilder))
                        .collect(Collectors.toList());
                    swapSrcTargetId = false;
                }
                projects.addAll(rexRightNodeMap);
            } else {
                RelDataTypeField field = afterRight.getPathSchema().getField(edgeName, true, false);
                PathInputRef edgeRef = new PathInputRef(edgeName, field.getIndex(), field.getType());
                for (int i = 0; i < rightInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(edgeRef, i));
                }
            }
            matchNode = afterRight;
        } else {
            GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
            GeaFlowGraph graph = typeFactory.getCurrentGraph();
            GraphDescriptor graphDescriptor = graph.getDescriptor();
            Optional<EdgeDescriptor> edgeDesc = graphDescriptor.edges.stream().filter(
                e -> e.type.equals(edgeTable.getName())).findFirst();
            VertexTable dummyVertex = null;
            if (edgeDesc.isPresent()) {
                EdgeDescriptor edgeDescriptor = edgeDesc.get();
                String dummyNodeType = direction.equals(EdgeDirection.IN)
                    ? edgeDescriptor.sourceType : edgeDescriptor.targetType;
                dummyVertex = graph.getVertexTables().stream().filter(
                    v -> v.getName().equals(dummyNodeType)).findFirst().get();
            }
            if (dummyVertex == null) {
                return null;
            }
            String dummyNodeName = dummyVertex.getName();
            RelDataType dummyVertexRelType = dummyVertex.getRowType(call.builder().getTypeFactory());
            pathRecordType = pathRecordType.addField(dummyNodeName, dummyVertexRelType, true);
            VertexMatch dummyVertexMatch = VertexMatch.create(cluster, null, dummyNodeName,
                Collections.singletonList(dummyVertex.getName()), dummyVertexRelType, pathRecordType);
            pathRecordType = pathRecordType.addField(edgeName, edgeRelType, true);
            EdgeDirection reverseDirection = EdgeDirection.reverse(direction);
            edgeMatch = EdgeMatch.create(cluster, dummyVertexMatch, edgeName,
                Collections.singletonList(edgeTable.getName()),
                reverseDirection, edgeRelType, pathRecordType);
            swapSrcTargetId = reverseDirection.equals(EdgeDirection.IN);
            IMatchNode afterLeft = concatToMatchNode(call.builder(), null, leftInput, leftHead,
                edgeMatch, rexLeftNodeMap);

            //Add edge fields
            if (rexLeftNodeMap.size() > 0) {
                // In the case of converting match out edges to in edges, swap the source and
                // target id references of the edge.
                if (swapSrcTargetId) {
                    rexLeftNodeMap = rexLeftNodeMap.stream()
                        .map(rex -> GQLRexUtil.swapReverseEdgeRef(rex, edgeName, rexBuilder))
                        .collect(Collectors.toList());
                    swapSrcTargetId = false;
                }
                projects.addAll(rexLeftNodeMap);
            } else {
                RelDataTypeField field = afterLeft.getPathSchema().getField(edgeName, true, false);
                PathInputRef edgeRef = new PathInputRef(edgeName, field.getIndex(), field.getType());
                for (int i = 0; i < leftInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(edgeRef, i));
                }
            }
            leftFieldCount = projects.size();

            pathRecordType = afterLeft.getPathSchema();
            pathRecordType = pathRecordType.addField(nodeName, vertexRelType, false);
            if (join.getJoinType().equals(JoinRelType.LEFT)) {
                vertexMatch = OptionalVertexMatch.create(cluster, (SingleMatchNode) afterLeft, nodeName,
                    Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            } else {
                vertexMatch = VertexMatch.create(cluster, (SingleMatchNode) afterLeft, nodeName,
                    Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            }

            IMatchNode afterRight = concatToMatchNode(call.builder(), afterLeft, rightInput, rightHead,
                vertexMatch, rexRightNodeMap);
            //Add vertex fields
            if (rexRightNodeMap.size() > 0) {
                projects.addAll(rexRightNodeMap);
            } else {
                RelDataTypeField field = afterRight.getPathSchema().getField(nodeName, true, false);
                PathInputRef vertexRef = new PathInputRef(nodeName, field.getIndex(), field.getType());
                for (int i = 0; i < rightInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(vertexRef, i));
                }
            }
            matchNode = afterRight;
        }

        //In the case of reverse matching in the IN direction, the positions of the source
        // vertex and the destination vertex are swapped.
        if (swapSrcTargetId) {
            int edgeSrcIdIndex = edgeRelType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                        && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int edgeTargetIdIndex = edgeRelType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                        && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_TARGET_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int baseOffset = isLeftTableVertex ? leftFieldCount : 0;
            Collections.swap(projects, baseOffset + edgeSrcIdIndex, baseOffset + edgeTargetIdIndex);
        }

        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph graph = typeFactory.getCurrentGraph();
        LogicalGraphScan graphScan = LogicalGraphScan.create(cluster, graph);
        LogicalGraphMatch graphMatch = LogicalGraphMatch.create(cluster, graphScan,
            matchNode, matchNode.getPathSchema());

        List<RelDataTypeField> matchTypeFields = new ArrayList<>();
        List<String> newFieldNames = this.generateFieldNames("f", projects.size(), new HashSet<>());
        for (int i = 0; i < projects.size(); i++) {
            matchTypeFields.add(new RelDataTypeFieldImpl(newFieldNames.get(i), i ,
                projects.get(i).getType()));
        }
        RelNode tail = LogicalProject.create(graphMatch, projects, new RelRecordType(matchTypeFields));

        // Complete the Join projection.
        final RelNode finalTail = tail;
        List<RexNode> joinProjects = IntStream.range(0, projects.size())
            .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i)).collect(Collectors.toList());
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
}
