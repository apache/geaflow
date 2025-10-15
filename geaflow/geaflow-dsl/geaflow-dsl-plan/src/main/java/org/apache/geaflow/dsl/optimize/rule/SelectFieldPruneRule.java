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

import java.util.*;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;


public class SelectFieldPruneRule extends RelOptRule {

    public static final SelectFieldPruneRule PROJECT_INSTANCE;
    public static final SelectFieldPruneRule GRAPH_MATCH_INSTANCE;

    static {
        PROJECT_INSTANCE = new ProjectPruneRule(LogicalProject.class);
        GRAPH_MATCH_INSTANCE = new GraphMatchPruneRule(LogicalGraphMatch.class);
    }

    //尝试通过thread判断是否访问过
    public static void resetState() {
        ProjectPruneRule.visitedProjectsThreadLocal.remove();
    }

    //将只有index作为索引转换为带label的完整fields
    private static List<RexFieldAccess> convertToPathRefs(List<RexFieldAccess> fieldAccesses, RexBuilder rexBuilder, PathRecordType pathRecordType) {
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

    // 内部类：处理 LogicalProject 的规则并下推
    private SelectFieldPruneRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    private static void transverseFilteredElements(List<RexFieldAccess> fields, IMatchNode pathPattern) {
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
                for (RexFieldAccess fieldElement: fields) {
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

        // 使用 ThreadLocal 替代实例字段
        private static final ThreadLocal<Set<RelDataType>> visitedProjectsThreadLocal =
                ThreadLocal.withInitial(() -> Collections.newSetFromMap(new IdentityHashMap<>()));

        private ProjectPruneRule(Class<? extends LogicalProject> clazz) {
            super(operand(clazz, operand(LogicalGraphMatch.class, any())),
                    "SelectFieldPruneRule(Project2GraphMatch)");
        }


        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);           // 获取 LogicalProject
            LogicalGraphMatch graphMatch = call.rel(1);     // 获取 LogicalGraphMatch (直接子节点)

            // 从 ThreadLocal 获取当前线程的 visitedProjects
            Set<RelDataType> visitedProjects = visitedProjectsThreadLocal.get();

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
            List<RexFieldAccess> fieldAccesses = new ArrayList<>();
            for (RexNode node : fields) {
                if (node instanceof RexFieldAccess) { //如果可以直接转换
                    fieldAccesses.add((RexFieldAccess) node);
                } else if (node instanceof RexCall) {  //自定义方法，需要提取元素转换
                    RexCall rexCall = (RexCall) node;
                    RexNode ref = rexCall.getOperands().get(0); //获取哪个表
                    String fieldName = rexCall.getOperator().getName();  //获取字段名
                    fieldName = "id".equals(fieldName)     ? "~id" :  //特殊字段手动替换
                                "label".equals(fieldName)  ? "~label" :
                                "srcId".equals(fieldName)  ? "~srcId" :
                                "targetId".equals(fieldName)  ? "~targetId" :
                                fieldName;
                    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
                    fieldAccesses.add((RexFieldAccess) rexBuilder.makeFieldAccess(ref, fieldName, false));
                } else {
                    throw new IllegalArgumentException("Unsupported type: " + node.getClass());
                }
            }

            RelDataType inputRowType = project.getInput(0).getRowType();
            PathRecordType pathRecordType = new PathRecordType(inputRowType.getFieldList());
            //PathRecordType pathRecordType = (PathRecordType) project.getRowType();
            return SelectFieldPruneRule.convertToPathRefs(fieldAccesses, project.getCluster().getRexBuilder(), pathRecordType);
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
            List<RexFieldAccess> filteredElements = getFilteredElements(graphMatch);

            // 2. 将筛选信息传递给 LogicalGraphMatch
            if (!filteredElements.isEmpty()) {
                transverseFilteredElements(filteredElements, graphMatch.getPathPattern());
            }
        }


        public List<RexFieldAccess> getFilteredElements(GraphMatch graphMatch) {
            List<RexFieldAccess> rawFields = extractFromMatchNode(graphMatch.getPathPattern()); //递归提取condition中的属性使用

            PathRecordType pathRecordType = (PathRecordType) graphMatch.getRowType();  //获取边的Type类型
            return SelectFieldPruneRule.convertToPathRefs(rawFields, graphMatch.getCluster().getRexBuilder(), pathRecordType);
        }

        /**
         * Recursively traverses the MatchNode to extract RexFieldAccess.
         */
        private List<RexFieldAccess> extractFromMatchNode(IMatchNode matchNode) {
            //这里只会提取MatchFilter的字段
            IMatchNode currentNode = matchNode;
            List<RexFieldAccess> allFilteredFields = new ArrayList<>();

            while (currentNode != null) { //这里默认所有RexNode均为线性结构，不存在多个children
                if (currentNode instanceof MatchFilter) {
                    MatchFilter filterNode = (MatchFilter) currentNode;
                    allFilteredFields.addAll(extractFromRexNode(filterNode.getCondition()));
                }

                if (currentNode.getInputs() == null || currentNode.getInputs().isEmpty()) {
                    break; // 没有子节点，退出循环
                }
                currentNode = (IMatchNode) currentNode.getInput(0);
            }
            return allFilteredFields;
        }

        /**
         * Extract RexFieldAccess from the target node.
         */
        private List<RexFieldAccess> extractFromRexNode(RexNode rexNode) {
            List<RexFieldAccess> fields = new ArrayList<>();
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                for (RexNode operand : rexCall.getOperands()) {
                    if (operand instanceof RexFieldAccess) {
                        fields.add((RexFieldAccess) operand);
                    } else if (operand instanceof RexCall) {
                        // 递归处理嵌套的RexCall
                        fields.addAll(extractFromRexNode(operand));
                    }
                }
            }
            return fields;
        }
    }
}
