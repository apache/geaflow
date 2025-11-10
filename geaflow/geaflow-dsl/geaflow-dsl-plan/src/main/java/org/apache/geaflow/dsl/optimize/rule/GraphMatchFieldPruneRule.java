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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.geaflow.dsl.rel.GraphMatch;
import org.apache.geaflow.dsl.rel.PathModify.PathModifyExpression;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.*;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;

/**
 * Rule to prune unnecessary fields within GraphMatch operations by analyzing
 * filter conditions, path modifications, joins, and extends.
 */
public class GraphMatchFieldPruneRule extends RelOptRule {

    public static final GraphMatchFieldPruneRule INSTANCE = new GraphMatchFieldPruneRule();

    private GraphMatchFieldPruneRule() {
        // Match only a single LogicalGraphMatch node
        super(operand(LogicalGraphMatch.class, any()), "GraphMatchFieldPruneRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalGraphMatch graphMatch = call.rel(0);

        // 1. Extract field access information from LogicalGraphMatch
        Set<RexFieldAccess> filteredElements = getFilteredElements(graphMatch);

        // 2. Pass the filtered field information to the path pattern
        if (!filteredElements.isEmpty()) {
            traverseAndPruneFields(filteredElements, graphMatch.getPathPattern());
        }
    }

    /**
     * Extract filtered field elements from GraphMatch.
     */
    public Set<RexFieldAccess> getFilteredElements(GraphMatch graphMatch) {
        // Recursively extract field usage from conditions in the match node
        return extractFromMatchNode(graphMatch.getPathPattern());
    }

    /**
     * Recursively traverse the MatchNode to extract RexFieldAccess.
     */
    private Set<RexFieldAccess> extractFromMatchNode(IMatchNode matchNode) {
        Set<RexFieldAccess> allFilteredFields = new HashSet<>();

        if (matchNode == null) {
            return allFilteredFields;
        }

        // Process expressions in the current node
        if (matchNode instanceof MatchFilter) {
            MatchFilter filterNode = (MatchFilter) matchNode;
            Set<RexFieldAccess> rawFields = extractFromRexNode(filterNode.getCondition());
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
            // For MatchExtend, check CAST attributes
            MatchExtend extendNode = (MatchExtend) matchNode;
            for (PathModifyExpression expression : extendNode.getExpressions()) {
                for (RexNode extendOperands : expression.getObjectConstruct().getOperands()) {
                    if (extendOperands instanceof RexCall) {
                        // Only consider non-primitive property projections (CAST)
                        Set<RexFieldAccess> rawFields = extractFromRexNode(extendOperands);
                        allFilteredFields.addAll(convertToPathRefs(rawFields, extendNode));
                    }
                }
            }
        }

        // Recursively process all child nodes
        if (matchNode.getInputs() != null && !matchNode.getInputs().isEmpty()) {
            for (RelNode input : matchNode.getInputs()) {
                if (input instanceof IMatchNode) {
                    // Conversion is handled at leaf nodes, so no need for convertToPathRefs here
                    allFilteredFields.addAll(extractFromMatchNode((IMatchNode) input));
                }
            }
        }

        return allFilteredFields;
    }

    /**
     * Extract RexFieldAccess from the target RexNode.
     */
    private Set<RexFieldAccess> extractFromRexNode(RexNode rexNode) {
        Set<RexFieldAccess> fields = new HashSet<>();

        if (rexNode instanceof RexLiteral || rexNode instanceof RexInputRef) {
            return fields;
        } else {
            RexCall rexCall = (RexCall) rexNode;
            for (RexNode operand : rexCall.getOperands()) {
                if (operand instanceof RexFieldAccess) {
                    fields.add((RexFieldAccess) operand);
                } else if (operand instanceof RexCall) {
                    // Recursively process nested RexCall
                    fields.addAll(extractFromRexNode(operand));
                }
            }
        }
        return fields;
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
                if (candidateInput != null && !visited.contains((IMatchNode) candidateInput)) {
                    queue.offer((IMatchNode) candidateInput);
                    visited.add((IMatchNode) candidateInput);
                }
            }
        }
    }
}
