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

package org.apache.geaflow.dsl.rel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.LoopUntilMatch;
import org.apache.geaflow.dsl.rel.match.MatchAggregate;
import org.apache.geaflow.dsl.rel.match.MatchDistinct;
import org.apache.geaflow.dsl.rel.match.MatchExtend;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.MatchPathModify;
import org.apache.geaflow.dsl.rel.match.MatchPathSort;
import org.apache.geaflow.dsl.rel.match.MatchSharedPredicate;
import org.apache.geaflow.dsl.rel.match.MatchUnion;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rel.match.VirtualEdgeMatch;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public abstract class GraphMatch extends SingleRel {

    protected final IMatchNode pathPattern;

    protected GraphMatch(RelOptCluster cluster, RelTraitSet traits,
                         RelNode input, IMatchNode pathPattern, RelDataType rowType) {
        super(cluster, traits, input);
        this.rowType = Objects.requireNonNull(rowType);
        this.pathPattern = Objects.requireNonNull(pathPattern);
        validateInput(input);
    }

    private void validateInput(RelNode input) {
        SqlTypeName inputType = input.getRowType().getSqlTypeName();
        if (inputType != SqlTypeName.GRAPH && inputType != SqlTypeName.PATH) {
            throw new GeaFlowDSLException("Illegal input type:" + inputType
                + ", for " + getRelTypeName());
        }
    }

    public boolean canConcat(IMatchNode pathPattern) {
        return GQLRelUtil.isAllSingleMatch(this.pathPattern)
            && GQLRelUtil.isAllSingleMatch(pathPattern)
            && this.pathPattern.getPathSchema().canConcat(pathPattern.getPathSchema())
            ;
    }

    /**
     * Merge with a path pattern to generate a new graph match node.
     */
    public GraphMatch merge(IMatchNode pathPattern) {
        if (canConcat(pathPattern)) {
            SingleMatchNode concatPathPattern = GQLRelUtil.concatPathPattern(
                (SingleMatchNode) this.pathPattern, (SingleMatchNode) pathPattern, true);

            return this.copy(getTraitSet(), input, concatPathPattern, concatPathPattern.getPathSchema());
        } else {
            RexNode condition = GQLRelUtil.createPathJoinCondition(this.pathPattern, pathPattern,
                true, getCluster().getRexBuilder());
            MatchJoin join = MatchJoin.create(getCluster(), getTraitSet(),
                this.pathPattern, pathPattern, condition, JoinRelType.INNER);

            return this.copy(getTraitSet(), input, join, join.getRowType());
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        ExplainVisitor explainVisitor = new ExplainVisitor();
        String path = explainVisitor.visit(pathPattern);

        return super.explainTerms(pw)
            .item("path", path);
    }

    public static class ExplainVisitor extends AbstractMatchNodeVisitor<String> {

        @Override
        public String visitVertexMatch(VertexMatch vertexMatch) {
            String inputString = "";
            if (vertexMatch.getInput() != null) {
                inputString = visit(vertexMatch.getInput());
            }
            String nodeString = "(" + vertexMatch.getLabel() + ":"
                + StringUtils.join(vertexMatch.getTypes(), "|") + ")";
            return inputString + nodeString;
        }

        @Override
        public String visitEdgeMatch(EdgeMatch edgeMatch) {
            String inputString = "";
            if (edgeMatch.getInput() != null) {
                inputString = visit(edgeMatch.getInput()) + "-";
            }
            String direction;
            switch (edgeMatch.getDirection()) {
                case OUT:
                    direction = "->";
                    break;
                case IN:
                    direction = "<-";
                    break;
                case BOTH:
                    direction = "-";
                    break;
                default:
                    throw new IllegalArgumentException("Illegal edge direction: " + edgeMatch.getDirection());
            }
            String nodeString = "[" + edgeMatch.getLabel() + ":"
                + StringUtils.join(edgeMatch.getTypes(), "|")
                + "]" + direction;
            return inputString + nodeString;
        }

        @Override
        public String visitVirtualEdgeMatch(VirtualEdgeMatch virtualEdgeMatch) {
            String inputString = visit(virtualEdgeMatch.getInput()) + "-";
            String nodeString = "[:"
                + " targetId=" + virtualEdgeMatch.getTargetId() + "]--";
            return inputString + nodeString;
        }

        @Override
        public String visitFilter(MatchFilter filter) {
            return visit(filter.getInput()) + " where " + filter.getCondition() + " ";
        }

        @Override
        public String visitJoin(MatchJoin join) {
            return "{" + visit(join.getLeft()) + "} Join {" + visit(join.getRight()) + "}";
        }

        @Override
        public String visitDistinct(MatchDistinct distinct) {
            return visit(distinct.getInput()) + " Distinct" + "{" + visit(distinct.getInput()) + "}";
        }

        @Override
        public String visitUnion(MatchUnion union) {
            return union.getInputs().stream()
                .map(this::visit)
                .map(explain -> "{" + explain + "}")
                .collect(Collectors.joining(" Union "));
        }

        @Override
        public String visitSharedPredicate(MatchSharedPredicate sharedPredicate) {
            String leftString = visit(sharedPredicate.getLeft());
            String rightString = visit(sharedPredicate.getRight());
            String operator = sharedPredicate.isDistinct() ? " | " : " |+| ";
            return "{" + leftString + "}" + operator + "{" + rightString + "} WHERE SHARED(" + sharedPredicate.getCondition() + ")";
        }

        @Override
        public String visitLoopMatch(LoopUntilMatch loopMatch) {
            String inputString = visit(loopMatch.getInput()) + "-";
            return inputString + " loop(" + visit(loopMatch.getLoopBody()) + ")"
                + ".time(" + loopMatch.getMinLoopCount() + "," + loopMatch.getMaxLoopCount() + ")"
                + ".until(" + loopMatch.getUtilCondition() + ")";
        }

        @Override
        public String visitSubQueryStart(SubQueryStart subQueryStart) {
            return "";
        }

        @Override
        public String visitPathModify(MatchPathModify pathModify) {
            return visit(pathModify.getInput()) + " PathModify(" + pathModify.expressions + ")";
        }

        @Override
        public String visitExtend(MatchExtend matchExtend) {
            return visit(matchExtend.getInput()) + " MatchExtend(" + matchExtend.expressions + ")";
        }

        @Override
        public String visitSort(MatchPathSort pathSort) {
            return visit(pathSort.getInput()) + " order by "
                + StringUtils.join(pathSort.orderByExpressions, ",")
                + " limit " + pathSort.getLimit();
        }

        @Override
        public String visitAggregate(MatchAggregate matchAggregate) {
            return visit(matchAggregate.getInput()) + " aggregate ";
        }
    }

    public IMatchNode getPathPattern() {
        return pathPattern;
    }

    public abstract GraphMatch copy(RelTraitSet traitSet, RelNode input, IMatchNode pathPattern, RelDataType rowType);

    public GraphMatch copy(IMatchNode pathPattern) {
        return copy(traitSet, input, pathPattern, pathPattern.getRowType());
    }

    @Override
    public GraphMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, inputs.get(0), pathPattern, rowType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return copy(traitSet, input, (IMatchNode) pathPattern.accept(shuttle), rowType);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return copy(traitSet, input, (IMatchNode) pathPattern.accept(shuttle), rowType);
    }
}
