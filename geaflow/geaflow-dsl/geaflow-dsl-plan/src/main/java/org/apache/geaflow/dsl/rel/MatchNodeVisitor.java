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

import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
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
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rel.match.VirtualEdgeMatch;

public interface MatchNodeVisitor<T> {

    T visitVertexMatch(VertexMatch vertexMatch);

    T visitEdgeMatch(EdgeMatch edgeMatch);

    T visitVirtualEdgeMatch(VirtualEdgeMatch virtualEdgeMatch);

    T visitFilter(MatchFilter filter);

    T visitJoin(MatchJoin join);

    T visitDistinct(MatchDistinct distinct);

    T visitUnion(MatchUnion union);

    T visitLoopMatch(LoopUntilMatch loopMatch);

    T visitSubQueryStart(SubQueryStart subQueryStart);

    T visitPathModify(MatchPathModify pathModify);

    T visitExtend(MatchExtend matchExtend);

    T visitSort(MatchPathSort pathSort);

    T visitAggregate(MatchAggregate matchAggregate);

    T visitSharedPredicate(MatchSharedPredicate sharedPredicate);

    T visit(RelNode node);
}
