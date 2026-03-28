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

import static org.apache.geaflow.dsl.common.types.EdgeType.*;
import static org.apache.geaflow.dsl.common.types.VertexType.DEFAULT_ID_FIELD_NAME;

import java.util.*;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.*;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexParameterRef;
import org.apache.geaflow.dsl.util.GQLRelUtil;

/**
 * Rule to prune unnecessary fields from LogicalProject and push down field requirements
 * to LogicalGraphMatch.
 */
public class ProjectFieldPruneRule extends RelOptRule {

    public static final ProjectFieldPruneRule INSTANCE = new ProjectFieldPruneRule();

    // Mapping for special field names
    private static final Map<String, String> SPECIAL_FIELD_MAP;

    static {
        SPECIAL_FIELD_MAP = new HashMap<>();
        SPECIAL_FIELD_MAP.put("id", DEFAULT_ID_FIELD_NAME);
        SPECIAL_FIELD_MAP.put("label", DEFAULT_LABEL_NAME);
        SPECIAL_FIELD_MAP.put("srcId", DEFAULT_SRC_ID_NAME );
        SPECIAL_FIELD_MAP.put("targetId", DEFAULT_TARGET_ID_NAME);
    }

    private ProjectFieldPruneRule() {
        super(operand(LogicalProject.class, operand(LogicalGraphMatch.class, any())),
                "ProjectFieldPruneRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);           // Get LogicalProject
        LogicalGraphMatch graphMatch = call.rel(1);     // Get LogicalGraphMatch (direct child)

        // 1. Extract field access information from LogicalProject
        Set<RexFieldAccess> filteredElements = extractFields(project);

        // 2. Pass the filtered field information to LogicalGraphMatch
        if (!filteredElements.isEmpty()) {
            traverseAndPruneFields(filteredElements, graphMatch.getPathPattern());
        }
    }

    /**
     * Extract fields from LogicalProject and convert to semantic information (e.g., $0.id -> a.id).
     */
    private Set<RexFieldAccess> extractFields(LogicalProject project) {
        List<RexNode> fields = project.getChildExps();
        Set<RexFieldAccess> fieldAccesses = new HashSet<>();

        for (RexNode node : fields) {
            // Recursively collect all field accesses
            fieldAccesses.addAll(collectAllFieldAccesses(
                    project.getCluster().getRexBuilder(), node));
        }

        // Convert index-based references to label-based path references
        return convertToPathRefs(fieldAccesses, GQLRelUtil.toRel(project.getInput(0)));
    }

    /**
     * Recursively collect all RexFieldAccess nodes from a RexNode tree.
     */
    private static Set<RexFieldAccess> collectAllFieldAccesses(RexBuilder rexBuilder, RexNode rootNode) {
        Set<RexFieldAccess> fieldAccesses = new HashSet<>();
        Queue<RexNode> queue = new LinkedList<>();
        queue.offer(rootNode);

        while (!queue.isEmpty()) {
            RexNode node = queue.poll();

            if (node instanceof RexFieldAccess) {
                // Direct field access
                fieldAccesses.add((RexFieldAccess) node);

            } else if (node instanceof RexCall) {
                // Custom function call, need to extract and convert elements
                RexCall rexCall = (RexCall) node;

                // Check if it's a field access type call (operand[0] is ref, operator is field name)
                if (rexCall.getOperands().size() > 0) {
                    RexNode ref = rexCall.getOperands().get(0);
                    String fieldName = rexCall.getOperator().getName();

                    // Handle special fields with mapping
                    if (SPECIAL_FIELD_MAP.containsKey(fieldName)) {
                        String mappedFieldName = SPECIAL_FIELD_MAP.get(fieldName);
                        fieldAccesses.add((RexFieldAccess) rexBuilder.makeFieldAccess(ref, mappedFieldName, false));

                    } else if (ref instanceof RexInputRef) {
                        // Other non-nested custom functions: enumerate all fields of ref and add them all
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
                        // ref itself might be a complex expression, continue recursive processing
                        queue.add(ref);
                    }

                    // Add other operands to the queue for continued processing
                    for (int i = 1; i < rexCall.getOperands().size(); i++) {
                        queue.add(rexCall.getOperands().get(i));
                    }
                }

            } else if (node instanceof RexInputRef) {
                // RexInputRef directly references input, enumerate all its fields
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
                // Literals, skip
                continue;

            } else {
                // Other unknown types, can choose to throw exception or log
                throw new IllegalArgumentException("Unsupported type: " + node.getClass());
            }
        }

        return fieldAccesses;
    }

    /**
     * Convert index-only field accesses to complete fields with labels.
     */
    private static Set<RexFieldAccess> convertToPathRefs(Set<RexFieldAccess> fieldAccesses, RelNode node) {
        Set<RexFieldAccess> convertedFieldAccesses = new HashSet<>();
        RelDataType pathRecordType = node.getRowType(); // Get the record type at current level
        RexBuilder rexBuilder = node.getCluster().getRexBuilder(); // Builder for creating new fields

        for (RexFieldAccess fieldAccess : fieldAccesses) {
            RexNode referenceExpr = fieldAccess.getReferenceExpr();

            // Only process field accesses of input reference type
            if (referenceExpr instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) referenceExpr;

                // If index exceeds field list size, it comes from a subquery, skip it
                if (pathRecordType.getFieldList().size() <= inputRef.getIndex()) {
                    continue;
                }

                // Get the corresponding path field information from PathRecordType
                RelDataTypeField pathField = pathRecordType.getFieldList().get(inputRef.getIndex());

                // Create the actual PathInputRef
                PathInputRef pathInputRef = new PathInputRef(
                        pathField.getName(),     // Path variable name (e.g., "a", "b", "c")
                        pathField.getIndex(),    // Field index
                        pathField.getType()      // Field type
                );

                // Recreate RexFieldAccess with the new path reference
                RexFieldAccess newFieldAccess = (RexFieldAccess) rexBuilder.makeFieldAccess(
                        pathInputRef,
                        fieldAccess.getField().getIndex()
                );
                convertedFieldAccesses.add(newFieldAccess);
            }
        }

        return convertedFieldAccesses;
    }

    /**
     * Traverse the path pattern and add filtered fields to matching nodes.
     */
    private static void traverseAndPruneFields(Set<RexFieldAccess> fields, IMatchNode pathPattern) {
        Queue<IMatchNode> queue = new LinkedList<>(); // Queue for nodes to visit
        Set<IMatchNode> visited = new HashSet<>();    // Mark visited nodes

        queue.offer(pathPattern);
        visited.add(pathPattern);

        // Visit all nodes in the path, and for each field: if label matches, add the field to .fields
        while (!queue.isEmpty()) {
            IMatchNode currentPathPattern = queue.poll();

            if (currentPathPattern instanceof VertexMatch) {
                VertexMatch vertexMatch = (VertexMatch) currentPathPattern;
                String vertexLabel = vertexMatch.getLabel();
                for (RexFieldAccess fieldElement : fields) {
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

            // Iterate through possible child nodes
            List<RelNode> inputs = currentPathPattern.getInputs();
            for (RelNode candidateInput : inputs) {
                if (candidateInput == null) {
                    continue;
                }
                RelNode input = GQLRelUtil.toRel(candidateInput);
                if (input instanceof IMatchNode) {
                    IMatchNode matchInput = (IMatchNode) input;
                    if (visited.add(matchInput)) {
                        queue.offer(matchInput);
                    }
                }
            }
        }
    }
}
