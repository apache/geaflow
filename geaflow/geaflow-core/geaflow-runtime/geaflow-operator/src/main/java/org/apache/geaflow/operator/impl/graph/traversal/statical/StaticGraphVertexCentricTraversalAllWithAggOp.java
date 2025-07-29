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

package org.apache.geaflow.operator.impl.graph.traversal.statical;

import org.apache.geaflow.api.graph.base.algo.AbstractVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import org.apache.geaflow.operator.impl.graph.algo.vc.GraphVertexCentricOpAggregator;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricAggOp;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class StaticGraphVertexCentricTraversalAllWithAggOp<K, VV, EV, M, R, I, PA, PR, GR,
    FUNC extends VertexCentricTraversalFunction<K, VV, EV, M, R> & VertexCentricAggContextFunction<I, GR>>
    extends StaticGraphVertexCentricTraversalAllOp<K, VV, EV, M, R, FUNC>
    implements IGraphVertexCentricAggOp<K, VV, EV, M, I, PA, PR, GR> {

    private GraphVertexCentricOpAggregator<K, VV, EV, M, I, ?, ?, ?, GR,
        VertexCentricAggTraversal<K, VV, EV, M, R, I, ?, ?, ?, GR>> aggregator;

    public StaticGraphVertexCentricTraversalAllWithAggOp(GraphViewDesc graphViewDesc,
                                                         AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        aggregator = new GraphVertexCentricOpAggregator(this);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        aggregator.open((VertexCentricAggContextFunction) vcTraversalFunction);
    }

    @Override
    public void initIteration(long iteration) {
        super.initIteration(iteration);
        aggregator.initIteration(iteration);
    }

    public void finishIteration(long iteration) {
        super.finishIteration(iteration);
        aggregator.finishIteration(iteration);
    }

    @Override
    public void processAggregateResult(GR result) {
        aggregator.processAggregateResult(result);
    }
}
