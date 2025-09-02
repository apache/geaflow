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
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
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
import org.apache.geaflow.dsl.rel.match.MatchSamePredicate;
import org.apache.geaflow.dsl.rel.match.MatchUnion;
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rel.match.VirtualEdgeMatch;

public class MatchRelShuffle extends AbstractMatchNodeVisitor<IMatchNode> {

    @Override
    public IMatchNode visitVertexMatch(VertexMatch vertexMatch) {
        return visitChildren(vertexMatch);
    }

    @Override
    public IMatchNode visitEdgeMatch(EdgeMatch edgeMatch) {
        return visitChildren(edgeMatch);
    }

    @Override
    public IMatchNode visitVirtualEdgeMatch(VirtualEdgeMatch virtualEdgeMatch) {
        return visitChildren(virtualEdgeMatch);
    }

    @Override
    public IMatchNode visitFilter(MatchFilter filter) {
        return visitChildren(filter);
    }

    @Override
    public IMatchNode visitJoin(MatchJoin join) {
        return visitChildren(join);
    }

    @Override
    public IMatchNode visitDistinct(MatchDistinct distinct) {
        return visitChildren(distinct);
    }

    @Override
    public IMatchNode visitUnion(MatchUnion union) {
        return visitChildren(union);
    }

    @Override
    public IMatchNode visitSamePredicate(MatchSamePredicate samePredicate) {
        return visitChildren(samePredicate);
    }

    @Override
    public IMatchNode visitLoopMatch(LoopUntilMatch loopMatch) {
        return visitChildren(loopMatch);
    }

    @Override
    public IMatchNode visitSubQueryStart(SubQueryStart subQueryStart) {
        return visitChildren(subQueryStart);
    }

    @Override
    public IMatchNode visitPathModify(MatchPathModify pathModify) {
        return visitChildren(pathModify);
    }

    @Override
    public IMatchNode visitExtend(MatchExtend matchExtend) {
        return visitChildren(matchExtend);
    }

    @Override
    public IMatchNode visitSort(MatchPathSort pathSort) {
        return visitChildren(pathSort);
    }

    @Override
    public IMatchNode visitAggregate(MatchAggregate matchAggregate) {
        return visitChildren(matchAggregate);
    }

    protected IMatchNode visitChildren(IMatchNode parent) {
        List<RelNode> newInputs = parent.getInputs().stream()
            .map(this::visit).collect(Collectors.toList());
        return (IMatchNode) parent.copy(parent.getTraitSet(), newInputs);
    }
}
