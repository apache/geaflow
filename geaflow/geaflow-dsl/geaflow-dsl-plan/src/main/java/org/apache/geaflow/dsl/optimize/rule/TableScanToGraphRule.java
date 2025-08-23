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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphScan;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class TableScanToGraphRule extends AbstractJoinToGraphRule {

    public static final TableScanToGraphRule INSTANCE = new TableScanToGraphRule();

    public TableScanToGraphRule() {
        super(operand(LogicalTableScan.class, any()));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode leftInput = call.rel(0);
        RelNode leftHead = null;
        RelNode leftTableScan = leftInput;
        while (!(leftTableScan instanceof LogicalTableScan)) {
            leftHead = leftTableScan;
            leftTableScan = GQLRelUtil.toRel(leftTableScan.getInput(0));
        }
        GeaFlowTable leftTable = leftTableScan.getTable().unwrap(GeaFlowTable.class);
        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph graph = typeFactory.getCurrentGraph();
        if (graph == null) {
            return;
        }
        if (!graph.containTable(leftTable)) {
            if (leftTable instanceof VertexTable || leftTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    leftTable.getName());
            }
            return;
        }
        RelNode tail;
        RelOptCluster cluster = leftTableScan.getCluster();
        LogicalGraphScan graphScan = LogicalGraphScan.create(cluster, graph);

        VertexMatch vertexMatch;
        List<RexNode> projects = new ArrayList<>();
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        List<RexNode> rexLeftNodeMap = new ArrayList<>();
        PathRecordType pathRecordType = PathRecordType.EMPTY;
        IMatchNode afterLeft;
        if (leftTable instanceof VertexTable) {
            VertexTable vertexTable = (VertexTable) leftTable;
            RelDataType vertexRelType = vertexTable.getRowType(call.builder().getTypeFactory());
            String nodeName = vertexTable.getName();

            pathRecordType = pathRecordType.addField(nodeName, vertexRelType, false);
            vertexMatch = VertexMatch.create(cluster, null, nodeName,
                Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            afterLeft = concatToMatchNode(call.builder(), null, leftInput, leftHead,
                vertexMatch, rexLeftNodeMap);
            //Add vertex fields
            if (!rexLeftNodeMap.isEmpty()) {
                projects.addAll(rexLeftNodeMap);
            } else {
                RelDataTypeField field = afterLeft.getPathSchema().getField(nodeName, true, false);
                PathInputRef vertexRef = new PathInputRef(nodeName, field.getIndex(), field.getType());
                for (int i = 0; i < leftInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(vertexRef, i));
                }
            }
            tail = afterLeft;
        } else {
            EdgeTable edgeTable = (EdgeTable) leftTable;
            GraphDescriptor graphDescriptor = graph.getDescriptor();
            Optional<EdgeDescriptor> edgeDesc = graphDescriptor.edges.stream().filter(
                e -> e.type.equals(edgeTable.getName())).findFirst();
            VertexTable dummyVertex = null;
            if (edgeDesc.isPresent()) {
                EdgeDescriptor edgeDescriptor = edgeDesc.get();
                String dummyNodeType = edgeDescriptor.sourceType;
                dummyVertex = graph.getVertexTables().stream().filter(
                    v -> v.getName().equals(dummyNodeType)).findFirst().get();
            }
            if (dummyVertex == null) {
                return;
            }
            String dummyNodeName = dummyVertex.getName();
            RelDataType dummyVertexRelType = dummyVertex.getRowType(call.builder().getTypeFactory());
            pathRecordType = pathRecordType.addField(dummyNodeName, dummyVertexRelType, true);
            VertexMatch dummyVertexMatch = VertexMatch.create(cluster, null, dummyNodeName,
                Collections.singletonList(dummyVertex.getName()), dummyVertexRelType, pathRecordType);
            RelDataType edgeRelType = edgeTable.getRowType(call.builder().getTypeFactory());
            String edgeName = edgeTable.getName();
            pathRecordType = pathRecordType.addField(edgeName, edgeRelType, true);
            IMatchNode edgeMatch = EdgeMatch.create(cluster, dummyVertexMatch, edgeName,
                Collections.singletonList(edgeTable.getName()),
                EdgeDirection.OUT, edgeRelType, pathRecordType);
            afterLeft = concatToMatchNode(call.builder(), null, leftInput, leftHead,
                edgeMatch, rexLeftNodeMap);
            tail = afterLeft;
            //Add edge fields
            if (!rexLeftNodeMap.isEmpty()) {
                projects.addAll(rexLeftNodeMap);
            } else {
                RelDataTypeField field = afterLeft.getPathSchema().getField(edgeName, true, false);
                PathInputRef edgeRef = new PathInputRef(edgeName, field.getIndex(), field.getType());
                for (int i = 0; i < leftInput.getRowType().getFieldCount(); i++) {
                    projects.add(rexBuilder.makeFieldAccess(edgeRef, i));
                }
            }
        }
        if (tail == null) {
            return;
        }
        LogicalGraphMatch graphMatch = LogicalGraphMatch.create(cluster, graphScan,
            afterLeft, afterLeft.getPathSchema());

        List<RelDataTypeField> matchTypeFields = new ArrayList<>();
        List<String> newFieldNames = this.generateFieldNames("f", projects.size(), new HashSet<>());
        for (int i = 0; i < projects.size(); i++) {
            matchTypeFields.add(new RelDataTypeFieldImpl(newFieldNames.get(i), i,
                projects.get(i).getType()));
        }
        tail = LogicalProject.create(graphMatch, projects, new RelRecordType(matchTypeFields));
        call.transformTo(tail);
    }
}
