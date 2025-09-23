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

package com.antgroup.geaflow.dsl.optimize.rule;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.match.*;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.*;
import java.util.stream.Collectors;


public class SelectFieldPruneRule extends RelOptRule {

    public static final SelectFieldPruneRule PROJECT_INSTANCE;
    public static final SelectFieldPruneRule GRAPH_MATCH_INSTANCE;

    static {
    PROJECT_INSTANCE = new ProjectPruneRule(LogicalProject.class);
    GRAPH_MATCH_INSTANCE = new GraphMatchPruneRule(LogicalGraphMatch.class);
    }

    // 内部类：处理 LogicalProject 的规则并下推
    private SelectFieldPruneRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    private static void transverseFilteredElements(List<RexFieldAccess> fields, IMatchNode pathPattern)
    {
        Queue<IMatchNode> queue = new LinkedList<>(); //记录待访问队列
        Set<IMatchNode> visited = new HashSet<>(); //标记已经访问过的点

        queue.offer(pathPattern);
        visited.add(pathPattern);

        //逐个访问该Path的所有点，同时遍历字段列表：若Label匹配则将该字段特征加入.fields
        while (!queue.isEmpty()) {
            IMatchNode currentPathPattern = queue.poll();

            if (currentPathPattern instanceof VertexMatch) {
                VertexMatch vertexMatch = (VertexMatch) currentPathPattern;
                String vertexLabel = vertexMatch.getLabel();
                for (RexFieldAccess fieldElement: fields){
                    PathInputRef inputRef = (PathInputRef) fieldElement.getReferenceExpr();
                    if (inputRef.getLabel().equals(vertexLabel)) {
                        vertexMatch.addField(fieldElement);
                    }
                }
            }
            if (currentPathPattern instanceof EdgeMatch) {
                EdgeMatch edgeMatch = (EdgeMatch) currentPathPattern;
                String edgeLabel = edgeMatch.getLabel();
                for (RexFieldAccess fieldElement : fields) {
                    PathInputRef inputRef = (PathInputRef) fieldElement.getReferenceExpr();
                    if (inputRef.getLabel().equals(edgeLabel)) {
                        edgeMatch.addField(fieldElement);
                    }
                }
            }

            //循环遍历可能存在的字段
            List<RelNode> inputs = currentPathPattern.getInputs();
            for (RelNode candidateInput : inputs) {
                if (candidateInput != null && !visited.contains((IMatchNode) candidateInput)) {
                    queue.offer((IMatchNode) candidateInput);
                    visited.add((IMatchNode) candidateInput);
                }
            }
        }

    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // 基类的onMatch方法，由子类重写具体逻辑
    }

    // 内部类：处理 LogicalProject 的规则
    private static class ProjectPruneRule extends SelectFieldPruneRule {

        private final Set<RelDataType> visitedProjects =
                Collections.newSetFromMap(new IdentityHashMap<>());
        private ProjectPruneRule(Class<? extends LogicalProject> clazz) {
            super(operand(clazz, operand(LogicalGraphMatch.class, any())),
                    "SelectFieldPruneRule(Project2GraphMatch)");
        }


        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);           // 获取 LogicalProject
            LogicalGraphMatch graphMatch = call.rel(1);     // 获取 LogicalGraphMatch (直接子节点)

            // 检查是否已经访问过，并标记
            if (visitedProjects.contains(project.getRowType())) {
                return;
            }
            visitedProjects.add(project.getRowType());

            // 1. 从 LogicalProject 中提取字段访问信息
            List<RexFieldAccess> filteredElements = extractFields(project);

            // 2. 将筛选信息传递给 LogicalGraphMatch
            if (!filteredElements.isEmpty()) {
                transverseFilteredElements(filteredElements, graphMatch.getPathPattern());
            }
        }

        //从LogicalProject中提取字段并转换为语义信息($0.id -> a.id)
        private List<RexFieldAccess> extractFields(LogicalProject project) {
            List<RexNode> fields = project.getChildExps();
            final List<RexFieldAccess> fieldAccesses = fields.stream()
                    .map(node -> (RexFieldAccess) node)
                    .collect(Collectors.toList());

            RelDataType inputRowType = project.getInput(0).getRowType();
            PathRecordType pathRecordType = new PathRecordType(inputRowType.getFieldList());
            //PathRecordType pathRecordType = ((IMatchNode) GQLRelUtil.toRel(project.getInput(0))).getPathSchema();
            return convertToPathRefs(fieldAccesses, project.getCluster().getRexBuilder(), pathRecordType);
        }

        private List<RexFieldAccess> convertToPathRefs(List<RexFieldAccess> fieldAccesses, RexBuilder rexBuilder, PathRecordType pathRecordType) {
            for (int i = 0; i < fieldAccesses.size(); i++) {
                RexFieldAccess fieldAccess = fieldAccesses.get(i);
                RexNode referenceExpr = fieldAccess.getReferenceExpr();

                // 只处理输入引用类型的字段访问
                if (referenceExpr instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) referenceExpr;

                    // 从 PathRecordType 中获取对应的路径字段信息
                    RelDataTypeField pathField = pathRecordType.getFieldList().get(inputRef.getIndex());

                    // 创建真正的 PathInputRef
                    PathInputRef pathInputRef = new PathInputRef(
                            pathField.getName(),     // 路径变量名 (如 "a", "b", "c")
                            pathField.getIndex(),    // 字段索引
                            pathField.getType()      // 字段类型
                    );

                    // 用新的路径引用重新创建 RexFieldAccess，替换原来的元素
                    RexFieldAccess newFieldAccess = (RexFieldAccess) rexBuilder.makeFieldAccess(
                            pathInputRef,
                            fieldAccess.getField().getIndex()
                    );
                    fieldAccesses.set(i, newFieldAccess);
                }
            }

            return fieldAccesses;
        }
    }


    // 内部类：处理 LogicalGraphMatch 的规则
    private static class GraphMatchPruneRule extends SelectFieldPruneRule {
        private GraphMatchPruneRule(Class<? extends LogicalGraphMatch> clazz) {
            // 只匹配单个 LogicalGraphMatch 节点
            super(operand(clazz, any()), "SelectFieldPruneRule(GraphMatch)");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalGraphMatch graphMatch = call.rel(0);
            // 处理 LogicalGraphMatch 的优化逻辑
            // 1. 从 LogicalGraphMatch 中提取字段访问信息
            List<RexFieldAccess> filteredElements = graphMatch.getFilteredElements();

            // 2. 将筛选信息传递给 LogicalGraphMatch
            if (filteredElements.isEmpty()) {
                transverseFilteredElements(filteredElements, graphMatch.getPathPattern());
            }
        }

        }
}
