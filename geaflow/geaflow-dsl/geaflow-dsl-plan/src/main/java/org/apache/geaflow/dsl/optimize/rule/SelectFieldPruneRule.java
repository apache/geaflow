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
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.PathModify.PathModifyExpression;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.*;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;
import org.apache.geaflow.dsl.rex.RexParameterRef;

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
    private static Set<RexFieldAccess> convertToPathRefs(Set<RexFieldAccess> fieldAccesses, RelNode node) {
        Set<RexFieldAccess> convertedFieldAccesses = new HashSet<>(); // 储存已经转换后的index
        RelDataType pathRecordType = node.getRowType(); // 获取当前层级下的表类型
        RexBuilder rexBuilder = node.getCluster().getRexBuilder(); // 通过构建函数新建新fileds

        for (RexFieldAccess fieldAccess: fieldAccesses) {
            RexNode referenceExpr = fieldAccess.getReferenceExpr();

            // 只处理输入引用类型的字段访问
            if (referenceExpr instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) referenceExpr;

                // 如果大于index，说明来源子查询，跳过
                if (pathRecordType.getFieldList().size() <= inputRef.getIndex()) {
                    continue;
                }

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
                convertedFieldAccesses.add(newFieldAccess);
            }
        }

        return convertedFieldAccesses;
    }

    // 从RexNode向下递归，挖掘所有可能的特征
    private static Set<RexFieldAccess> collectAllFieldAccesses(RexBuilder rexBuilder, RexNode rootNode) {
        Set<RexFieldAccess> fieldAccesses = new HashSet<>();
        Queue<RexNode> queue = new LinkedList<>();
        queue.offer(rootNode);

        while (!queue.isEmpty()) {
            RexNode node = queue.poll();

            if (node instanceof RexFieldAccess) {
                // 如果可以直接转换
                fieldAccesses.add((RexFieldAccess) node);

            } else if (node instanceof RexCall) {
                // 自定义方法，需要提取元素转换
                RexCall rexCall = (RexCall) node;

                // 检查是否是字段访问类型的调用（operand[0]是ref，operator是字段名）
                if (rexCall.getOperands().size() > 0) {
                    RexNode ref = rexCall.getOperands().get(0);
                    String fieldName = rexCall.getOperator().getName();

                    // 如果是特殊字段，按原来的映射处理
                    if ("id".equals(fieldName)
                            || "label".equals(fieldName)
                            || "srcId".equals(fieldName)
                            || "targetId".equals(fieldName)) {

                        String mappedFieldName = "id".equals(fieldName)     ? "~id" :
                                "label".equals(fieldName)  ? "~label" :
                                        "srcId".equals(fieldName)  ? "~srcId" :
                                                "~targetId";
                        fieldAccesses.add((RexFieldAccess) rexBuilder.makeFieldAccess(ref, mappedFieldName, false));

                    } else if (ref instanceof RexInputRef) {
                        // 其他非嵌套自定义函数：枚举ref的所有字段并全部加入
                        RelDataType refType = ref.getType();
                        List<RelDataTypeField> refFields = refType.getFieldList();

                        for (RelDataTypeField field : refFields) {
                            RexFieldAccess fieldAccess = (RexFieldAccess) rexBuilder.makeFieldAccess(
                                    ref,
                                    field.getName(),
                                    false
                            );
                            fieldAccesses.add(fieldAccess);
                        }

                    } else {
                        // ref本身可能是复杂表达式，继续递归处理
                        queue.add(ref);
                    }

                    // 将其他操作数也加入队列继续处理
                    for (int i = 1; i < rexCall.getOperands().size(); i++) {
                        queue.add(rexCall.getOperands().get(i));
                    }
                }

            } else if (node instanceof RexInputRef) {
                // RexInputRef 直接引用输入，枚举其所有字段
                RelDataType refType = node.getType();
                List<RelDataTypeField> refFields = refType.getFieldList();

                for (RelDataTypeField field : refFields) {
                    RexFieldAccess fieldAccess = (RexFieldAccess) rexBuilder.makeFieldAccess(
                            node,
                            field.getName(),
                            false
                    );
                    fieldAccesses.add(fieldAccess);
                }

            } else if (node instanceof RexLiteral || node instanceof RexParameterRef) {
                // 字面量，跳过
                continue;

            } else {
                // 其他未知类型，可以选择抛异常或记录日志
                throw new IllegalArgumentException("Unsupported type: " + node.getClass());
            }
        }

        return fieldAccesses;
    }

    // 内部类：处理 LogicalProject 的规则并下推
    private SelectFieldPruneRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    private static void transverseFilteredElements(Set<RexFieldAccess> fields, IMatchNode pathPattern) {
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
            Set<RexFieldAccess> filteredElements = extractFields(project);

            // 2. 将筛选信息传递给 LogicalGraphMatch
            if (!filteredElements.isEmpty()) {
                transverseFilteredElements(filteredElements, graphMatch.getPathPattern());
            }
        }

        //从LogicalProject中提取字段并转换为语义信息($0.id -> a.id)
        private Set<RexFieldAccess> extractFields(LogicalProject project) {
            List<RexNode> fields = project.getChildExps();
            Set<RexFieldAccess> fieldAccesses = new HashSet<>();
            for (RexNode node : fields) {
                fieldAccesses.addAll(collectAllFieldAccesses(
                        project.getCluster().getRexBuilder(), node)); // 递归获取所有信息
            }

            return SelectFieldPruneRule.convertToPathRefs(fieldAccesses, project.getInput(0));
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
            Set<RexFieldAccess> filteredElements = getFilteredElements(graphMatch);

            // 2. 将筛选信息传递给 LogicalGraphMatch
            if (!filteredElements.isEmpty()) {
                transverseFilteredElements(filteredElements, graphMatch.getPathPattern());
            }
        }


        public Set<RexFieldAccess> getFilteredElements(GraphMatch graphMatch) {
            return extractFromMatchNode(graphMatch.getPathPattern()); //递归提取condition中的属性使用
        }

        /**
         * Recursively traverses the MatchNode to extract RexFieldAccess.
         */
        private Set<RexFieldAccess> extractFromMatchNode(IMatchNode matchNode) {
            Set<RexFieldAccess> allFilteredFields = new HashSet<>();

            if (matchNode == null) {
                return allFilteredFields;
            }

            // 处理当前节点的表达式
            if (matchNode instanceof MatchFilter) {
                MatchFilter filterNode = (MatchFilter) matchNode;
                Set<RexFieldAccess> rawFields = extractFromRexNode(filterNode.getCondition()); // 可能为原始不含label的字段信息
                allFilteredFields.addAll(convertToPathRefs(rawFields, filterNode));

            } else if (matchNode instanceof MatchPathModify) {
                MatchPathModify pathModifyNode = (MatchPathModify) matchNode;
                RexObjectConstruct expression = pathModifyNode.getExpressions().get(0).getObjectConstruct();
                Set<RexFieldAccess> rawFields = extractFromRexNode(expression);
                allFilteredFields.addAll(convertToPathRefs(rawFields, matchNode));

            } else if (matchNode instanceof MatchJoin) {
                MatchJoin joinNode = (MatchJoin) matchNode;
                if (joinNode.getCondition() != null) {
                    Set<RexFieldAccess> rawFields = extractFromRexNode(joinNode.getCondition());
                    allFilteredFields.addAll(convertToPathRefs(rawFields, joinNode));
                }
            } else if (matchNode instanceof MatchExtend) {
                // 对于MatchExtend，需要检查CAST属性以
                MatchExtend extendNode = (MatchExtend) matchNode;
                for (PathModifyExpression expression: extendNode.getExpressions()) { // 枚举扩展的表达式
                    for (RexNode extendOperands: expression.getObjectConstruct().getOperands()) {
                        if (extendOperands instanceof RexCall) { // 只考虑非原始属性的投影(CAST)
                            Set<RexFieldAccess> rawFields = extractFromRexNode(extendOperands);
                            allFilteredFields.addAll(convertToPathRefs(rawFields, extendNode));
                        }
                    }
                }
            }

            // 递归处理所有子节点
            if (matchNode.getInputs() != null && !matchNode.getInputs().isEmpty()) {
                for (RelNode input : matchNode.getInputs()) {
                    if (input instanceof IMatchNode) {
                        // 在叶子节点处理转换，因此这里不需要`convertToPathRefs`
                        allFilteredFields.addAll(extractFromMatchNode((IMatchNode) input));
                    }
                }
            }

            return allFilteredFields;
        }

        /**
         * Extract RexFieldAccess from the target node.
         */
        private Set<RexFieldAccess> extractFromRexNode(RexNode rexNode) {
            Set<RexFieldAccess> fields = new HashSet<>();
            //if (rexNode instanceof RexCall) {
            if (rexNode instanceof RexLiteral || rexNode instanceof RexInputRef) {
                return fields;
            } else {
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



