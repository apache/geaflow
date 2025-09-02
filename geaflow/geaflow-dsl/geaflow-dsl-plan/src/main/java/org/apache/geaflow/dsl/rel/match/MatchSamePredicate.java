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

package org.apache.geaflow.dsl.rel.match;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;

/**
 * Relational operator for same predicate pattern matching.
 * This operator represents a pattern where two path patterns share a common predicate condition.
 *
 * The same predicate pattern is typically converted to a union + filter operation during optimization.
 */
public class MatchSamePredicate extends AbstractMatchNode {

    /**
     * Left path pattern
     */
    private final IMatchNode left;

    /**
     * Right path pattern
     */
    private final IMatchNode right;

    /**
     * Shared predicate condition that must be satisfied by both path patterns
     */
    private final RexNode condition;

    /**
     * Whether to use distinct semantics (true) or union all (false)
     */
    private final boolean isDistinct;

    /**
     * Constructor for MatchSamePredicate
     *
     * @param cluster the cluster
     * @param traits the trait set
     * @param left left path pattern
     * @param right right path pattern
     * @param condition shared predicate condition
     * @param isDistinct whether to use distinct semantics
     * @param pathSchema the path schema
     */
    protected MatchSamePredicate(RelOptCluster cluster, RelTraitSet traits,
                                  IMatchNode left, IMatchNode right,
                                  RexNode condition, boolean isDistinct,
                                  PathRecordType pathSchema) {
        super(cluster, traits, pathSchema);
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.isDistinct = isDistinct;
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) getRowType();
    }

    @Override
    public List<RelNode> getInputs() {
        return Arrays.asList(left, right);
    }

    @Override
    public RelDataType getNodeType() {
        return left.getNodeType();
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathType) {
        return new MatchSamePredicate(getCluster(), getTraitSet(),
            (IMatchNode) inputs.get(0), (IMatchNode) inputs.get(1),
            condition, isDistinct, pathType);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MatchSamePredicate(getCluster(), traitSet,
            (IMatchNode) inputs.get(0), (IMatchNode) inputs.get(1),
            condition, isDistinct, getPathSchema());
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitSamePredicate(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("left", left)
            .item("right", right)
            .item("condition", condition)
            .item("distinct", isDistinct);
    }

    /**
     * Get the left path pattern
     *
     * @return left path pattern
     */
    public IMatchNode getLeft() {
        return left;
    }

    /**
     * Get the right path pattern
     *
     * @return right path pattern
     */
    public IMatchNode getRight() {
        return right;
    }

    /**
     * Get the shared predicate condition
     *
     * @return predicate condition
     */
    public RexNode getCondition() {
        return condition;
    }

    /**
     * Check if this pattern uses distinct semantics
     *
     * @return true if distinct, false if union all
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * Check if this pattern uses union all semantics
     *
     * @return true if union all, false if distinct
     */
    public boolean isUnionAll() {
        return !isDistinct;
    }

    /**
     * Create a new MatchSamePredicate
     *
     * @param left left path pattern
     * @param right right path pattern
     * @param condition shared predicate condition
     * @param isDistinct whether to use distinct semantics
     * @param pathSchema the path schema
     * @return new MatchSamePredicate instance
     */
    public static MatchSamePredicate create(IMatchNode left, IMatchNode right,
                                            RexNode condition, boolean isDistinct,
                                            PathRecordType pathSchema) {
        return new MatchSamePredicate(left.getCluster(), left.getTraitSet(),
            left, right, condition, isDistinct, pathSchema);
    }
}
