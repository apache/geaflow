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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class LoopUntilMatch extends SingleRel implements SingleMatchNode {

    private final SingleMatchNode loopBody;

    private final int minLoopCount;

    private final int maxLoopCount;

    private final RexNode utilCondition;

    private final PathRecordType pathType;

    protected LoopUntilMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                             SingleMatchNode loopBody, RexNode utilCondition,
                             int minLoopCount, int maxLoopCount,
                             PathRecordType pathType) {
        super(cluster, traitSet, input);
        this.loopBody = Objects.requireNonNull(loopBody);
        this.utilCondition = Objects.requireNonNull(utilCondition);
        this.minLoopCount = minLoopCount;
        this.maxLoopCount = maxLoopCount;
        this.pathType = Objects.requireNonNull(pathType);
        this.rowType = pathType;
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return loopBody.getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitLoopMatch(this);
    }

    public SingleMatchNode getLoopBody() {
        return loopBody;
    }

    public RexNode getUtilCondition() {
        return utilCondition;
    }

    public int getMinLoopCount() {
        return minLoopCount;
    }

    public int getMaxLoopCount() {
        return maxLoopCount;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("body", loopBody)
            .item("minLoopCount", minLoopCount)
            .item("maxLoopCount", maxLoopCount)
            .item("condition", utilCondition);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new LoopUntilMatch(getCluster(), getTraitSet(), sole(inputs), loopBody,
            utilCondition, minLoopCount, maxLoopCount, pathSchema);
    }

    @Override
    public LoopUntilMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LoopUntilMatch(getCluster(), traitSet, sole(inputs), loopBody, utilCondition,
            minLoopCount, maxLoopCount, pathType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        GQLRelUtil.applyRexShuffleToTree(loopBody, shuttle);
        RexNode newUtilCondition = utilCondition.accept(shuttle);
        return new LoopUntilMatch(getCluster(), getTraitSet(), input, loopBody, newUtilCondition,
            minLoopCount, maxLoopCount, pathType);
    }


    public static LoopUntilMatch create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                        SingleMatchNode loopBody, RexNode utilCondition, int minLoopCount,
                                        int maxLoopCount, PathRecordType pathType) {
        return new LoopUntilMatch(cluster, traitSet, input, loopBody, utilCondition, minLoopCount,
            maxLoopCount, pathType);
    }

    public static SingleMatchNode copyWithSubQueryStartPathType(PathRecordType startPathType,
                                                                SingleMatchNode node,
                                                                boolean caseSensitive) {
        if (node == null) {
            return null;
        }
        if (node instanceof LoopUntilMatch) {
            LoopUntilMatch loop = (LoopUntilMatch) node;
            return LoopUntilMatch.create(loop.getCluster(), loop.getTraitSet(), loop.getInput(),
                copyWithSubQueryStartPathType(startPathType, loop.getLoopBody(), caseSensitive),
                loop.getUtilCondition(), loop.getMinLoopCount(),
                loop.getMaxLoopCount(), loop.getPathSchema());
        } else {
            PathRecordType concatPathType = startPathType;
            for (RelDataTypeField field : node.getPathSchema().getFieldList()) {
                if (concatPathType.getField(field.getName(), caseSensitive, false) == null) {
                    concatPathType = concatPathType.addField(field.getName(), field.getType(), caseSensitive);
                }
            }
            return (SingleMatchNode) node.copy(Lists.newArrayList(copyWithSubQueryStartPathType(
                startPathType, (SingleMatchNode) node.getInput(), caseSensitive)), concatPathType);
        }
    }

}
