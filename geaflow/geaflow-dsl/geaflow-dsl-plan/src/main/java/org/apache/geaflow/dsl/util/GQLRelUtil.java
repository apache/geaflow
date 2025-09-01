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

package org.apache.geaflow.dsl.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rel.MatchRelShuffle;
import org.apache.geaflow.dsl.rel.match.IMatchLabel;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.LoopUntilMatch;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rex.PathInputRef;

public class GQLRelUtil {

    public static IMatchNode match(RelNode node) {
        node = toRel(node);
        if (node instanceof IMatchNode) {
            return (IMatchNode) node;
        }
        throw new IllegalArgumentException("Node must be IMatchNode");
    }

    public static IMatchNode addSubQueryStartNode(IMatchNode root, SubQueryStart queryStart) {
        MatchRelShuffle shuffle = new MatchRelShuffle() {
            @Override
            protected IMatchNode visitChildren(IMatchNode parent) {
                List<RelNode> newInputs = new ArrayList<>();
                if (parent.getInputs().isEmpty()) {
                    newInputs.add(queryStart);
                } else {
                    for (RelNode input : parent.getInputs()) {
                        newInputs.add(visit(input));
                    }
                }
                return (IMatchNode) parent.copy(parent.getTraitSet(), newInputs);
            }
        };
        return shuffle.visit(root);
    }

    public static SingleMatchNode replaceInput(SingleMatchNode root, SingleMatchNode replacedNode,
                                               IMatchNode newInputNode) {
        if (root == replacedNode) {
            return (SingleMatchNode) root.copy(root.getTraitSet(), Collections.singletonList(newInputNode));
        } else {
            SingleMatchNode newInput = replaceInput((SingleMatchNode) root.getInput(), replacedNode, newInputNode);
            return (SingleMatchNode) root.copy(root.getTraitSet(), Collections.singletonList(newInput));
        }
    }

    public static List<RelNode> collect(RelNode root, Predicate<RelNode> predicate) {
        List<RelNode> childVisit = root.getInputs()
            .stream()
            .flatMap(input -> collect(input, predicate).stream())
            .collect(Collectors.toList());
        if (root instanceof LoopUntilMatch) {
            childVisit.addAll(collect(((LoopUntilMatch) root).getLoopBody(), predicate));
        }
        List<RelNode> results = new ArrayList<>(childVisit);
        if (predicate.test(root)) {
            results.add(root);
        }
        return results;
    }

    public static IMatchLabel getLatestMatchNode(SingleMatchNode pathPattern) {
        if (pathPattern == null) {
            return null;
        }
        if (pathPattern instanceof IMatchLabel) {
            return (IMatchLabel) pathPattern;
        }
        if (pathPattern instanceof LoopUntilMatch) {
            return getLatestMatchNode(((LoopUntilMatch) pathPattern).getLoopBody());
        }
        return getLatestMatchNode((SingleMatchNode) pathPattern.getInput());
    }

    public static IMatchLabel getFirstMatchNode(SingleMatchNode pathPattern) {
        if (pathPattern == null) {
            return null;
        }
        if (pathPattern.getInput() == null || pathPattern.getInput() instanceof SubQueryStart) {
            assert pathPattern instanceof IMatchLabel : "first node int match must be IMatchLabel.";
            return (IMatchLabel) pathPattern;
        }
        return getFirstMatchNode((SingleMatchNode) pathPattern.getInput());
    }

    public static boolean isAllSingleMatch(IMatchNode matchNode) {
        return collect(matchNode, node -> !(node instanceof SingleMatchNode)).isEmpty();
    }

    /**
     * Concat single path pattern "p0" to the start of path pattern "p1" if they can merge.
     * e.g. "a" is "(m) - (n) -(f)", "b" is "(f) - (p) - (q)"
     * Then we can concat them to "(m) - (n) - (f) - (p) - (q).
     */
    public static SingleMatchNode concatPathPattern(SingleMatchNode p0, SingleMatchNode p1, boolean caseSensitive) {
        assert isAllSingleMatch(p0) && isAllSingleMatch(p1);
        if (p1.getInput() == null) {
            // first node in single path pattern must be IMatchNode.
            assert p1 instanceof IMatchLabel;
            assert Objects.equals(((IMatchLabel) p1).getLabel(), getLatestMatchNode(p0).getLabel());
            IMatchLabel matchNode = (IMatchLabel) p1;
            Set<String> filterTypes = matchNode.getTypes();
            // since p1's label is same with p0, we can remove the duplicate label node p1 and only keep the
            // node type filters of p1.
            if (filterTypes.isEmpty()) {
                return p0;
            }
            return createNodeTypeFilter(p0, filterTypes);
        }
        SingleMatchNode concatInput = concatPathPattern(p0, (SingleMatchNode) p1.getInput(), caseSensitive);
        PathRecordType concatPathType = concatInput.getPathSchema();
        // generate new path schema.
        if (p1 instanceof IMatchLabel) {
            concatPathType = concatPathType.addField(((IMatchLabel) p1).getLabel(), p1.getNodeType(), caseSensitive);
        } else if (p1 instanceof LoopUntilMatch) {
            for (RelDataTypeField field : p1.getPathSchema().getFieldList()) {
                if (concatPathType.getField(field.getName(), caseSensitive, false) == null) {
                    concatPathType = concatPathType.addField(field.getName(), field.getType(), caseSensitive);
                }
            }
        }
        // copy with new input and path type.
        p1 = (SingleMatchNode) p1.copy(Lists.newArrayList(concatInput), concatPathType);
        if (p1 instanceof LoopUntilMatch) {
            LoopUntilMatch loop = (LoopUntilMatch) p1;
            p1 = LoopUntilMatch.copyWithSubQueryStartPathType(p0.getPathSchema(), loop, caseSensitive);
        }
        // adjust path field ref in match node after concat
        return (SingleMatchNode) p1.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                if (inputRef instanceof PathInputRef) {
                    int index = inputRef.getIndex();
                    int newIndex = index + p0.getPathSchema().getFieldCount() - 1;
                    return ((PathInputRef) inputRef).copy(newIndex);
                }
                return inputRef;
            }
        });
    }

    public static SingleMatchNode createNodeTypeFilter(SingleMatchNode input, Collection<String> nodeTypes) {
        RexBuilder rexBuilder = new RexBuilder(GQLJavaTypeFactory.create());
        SqlTypeName typeName = input.getNodeType().getSqlTypeName();
        RexNode nodeTypeRef;
        if (typeName == SqlTypeName.VERTEX) {
            nodeTypeRef = rexBuilder.makeInputRef(input.getNodeType().getFieldList()
                    .get(VertexType.LABEL_FIELD_POSITION).getType(),
                VertexType.LABEL_FIELD_POSITION);
        } else {
            assert typeName == SqlTypeName.EDGE;
            nodeTypeRef = rexBuilder.makeInputRef(input.getNodeType().getFieldList()
                    .get(EdgeType.LABEL_FIELD_POSITION).getType(),
                EdgeType.LABEL_FIELD_POSITION);
        }
        assert input.getPathSchema().lastField().isPresent();
        RelDataTypeField pathField = input.getPathSchema().lastField().get();
        nodeTypeRef = GQLRexUtil.toPathInputRefForWhere(pathField, nodeTypeRef);

        RexNode condition = null;
        for (String nodeType : nodeTypes) {
            RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, nodeTypeRef,
                GQLRexUtil.createString(nodeType));
            if (condition == null) {
                condition = eq;
            } else {
                condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, condition, eq);
            }
        }
        if (condition != null) {
            return MatchFilter.create(input, condition, input.getPathSchema());
        }
        return input;
    }

    public static Set<String> getLabels(RelNode node) {
        return collect(node, n -> n instanceof IMatchLabel)
            .stream().map(matchNode -> ((IMatchLabel) matchNode).getLabel())
            .collect(Collectors.toSet());
    }

    public static Set<String> getCommonLabels(RelNode node1, RelNode node2) {
        Set<String> nodeLabels1 = getLabels(node1);
        Set<String> nodeLabels2 = getLabels(node2);
        return Sets.intersection(nodeLabels1, nodeLabels2);
    }

    public static RelNode oneInput(List<RelNode> inputs) {
        if (inputs.size() == 0) {
            return null;
        }
        assert inputs.size() == 1 : "Node should have one input at most.";
        return inputs.get(0);
    }

    public static RelNode toRel(RelNode node) {
        if (node instanceof RelSubset) {
            return toRel(((RelSubset) node).getRelList().get(0));
        }
        if (node instanceof HepRelVertex) {
            return toRel(((HepRelVertex) node).getCurrentRel());
        }
        return node;
    }

    public static boolean findTableFunctionScan(RelNode rel) {
        rel = toRel(rel);
        if (rel instanceof LogicalTableFunctionScan) {
            return true;
        }
        if (rel instanceof LogicalCalc) {
            RelNode input = toRel(((LogicalCalc) rel).getInput());
            if (input instanceof LogicalTableFunctionScan) {
                return true;
            }
        }
        if (rel instanceof LogicalFilter) {
            RelNode input = toRel(((LogicalFilter) rel).getInput());
            return input instanceof LogicalTableFunctionScan;
        }
        return false;
    }

    public static boolean isGQLMatchRelNode(RelNode input) {
        RelNode node = toRel(input);
        return node.getRowType() instanceof PathRecordType;
    }

    public static RelNode applyRexShuffleToTree(RelNode node, RexShuttle rexShuttle) {
        if (node == null) {
            return null;
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        for (RelNode inputRel : node.getInputs()) {
            newInputs.add(applyRexShuffleToTree(inputRel, rexShuttle));
        }
        node = node.accept(rexShuttle);
        if (newInputs.isEmpty()) {
            return node;
        } else {
            return node.copy(node.getTraitSet(), newInputs);
        }
    }

    public static RexNode createPathJoinCondition(IMatchNode left, IMatchNode right,
                                                  boolean caseSensitive, RexBuilder rexBuilder) {
        Set<String> commonLabels = GQLRelUtil.getCommonLabels(left, right);
        List<RexNode> joinConditions = new ArrayList<>();

        for (String label : commonLabels) {
            RelDataTypeField leftField = left.getRowType().getField(label, caseSensitive, false);
            RelDataTypeField rightField = right.getRowType().getField(label, caseSensitive, false);
            RexInputRef leftRef = rexBuilder.makeInputRef(
                left.getPathSchema().getFieldList().get(leftField.getIndex()).getType(),
                leftField.getIndex());
            RexNode leftRefId = rexBuilder.makeFieldAccess(leftRef,
                VertexType.ID_FIELD_POSITION);
            RexInputRef rightRef = rexBuilder.makeInputRef(
                right.getPathSchema().getFieldList().get(rightField.getIndex()).getType(),
                rightField.getIndex() + left.getRowType().getFieldCount());
            RexNode rightRefId = rexBuilder.makeFieldAccess(rightRef,
                VertexType.ID_FIELD_POSITION);
            // create expression of "leftLabel.id = rightLabel.id".
            RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRefId, rightRefId);
            joinConditions.add(eq);
        }
        return RexUtil.composeConjunction(rexBuilder, joinConditions);
    }

    public static List<Integer> getRelNodeReference(RelNode relNode) {
        Set<Integer> references = new HashSet<>();
        relNode.accept(new RexShuttle() {
            @Override
            public RexInputRef visitInputRef(RexInputRef inputRef) {
                references.add(inputRef.getIndex());
                return inputRef;
            }
        });
        return references.stream().sorted().collect(Collectors.toList());
    }

    public static RelNode adjustInputRef(RelNode relNode, Map<Integer, Integer> indexMapping) {
        return relNode.accept(new RexShuttle() {
            @Override
            public RexInputRef visitInputRef(RexInputRef inputRef) {
                Integer newIndex = indexMapping.get(inputRef.getIndex());
                assert newIndex != null;
                if (inputRef instanceof PathInputRef) {
                    return ((PathInputRef) inputRef).copy(newIndex);
                }
                return new RexInputRef(newIndex, inputRef.getType());
            }
        });
    }

    /**
     * Reverse the traversal order for {@link SingleMatchNode}.
     *
     * @param matchNode The match node to reverse.
     * @param input     If input is not null, we make it as the input node of the reversed match node.
     */
    public static SingleMatchNode reverse(SingleMatchNode matchNode, IMatchNode input) {
        IMatchNode concatNode = input;
        SingleMatchNode node = matchNode;
        while (node != null) {
            SingleMatchNode endNode = node;
            // find the latest VertexMatch/EdgeMatch
            while (node != null && !(node instanceof IMatchLabel)) {
                node = (SingleMatchNode) node.getInput();
            }
            assert node != null;

            RelDataTypeField field = endNode.getPathSchema().lastField().get();
            if (concatNode != null) {
                PathRecordType pathRecordType = concatNode.getPathSchema().addField(field.getName()
                    , field.getType(), false);
                concatNode = node.copy(Collections.singletonList(concatNode), pathRecordType);
            } else {
                PathRecordType pathRecordType = PathRecordType.EMPTY.addField(field.getName()
                    , field.getType(), false);
                concatNode = node.copy(Collections.emptyList(), pathRecordType);
            }
            node = (SingleMatchNode) node.getInput();
        }
        return (SingleMatchNode) concatNode;
    }
}
