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

package org.apache.geaflow.operator.impl.graph.traversal.dynamic;

import static org.apache.geaflow.operator.Constants.GRAPH_VERSION;

import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class DynamicGraphVertexCentricTraversalAllOp<K, VV, EV, M, R,
    FUNC extends IncVertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractDynamicGraphVertexCentricTraversalOp<K, VV, EV, M, R, FUNC> {

    public DynamicGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
    }

    private void traversalEvolveVIds() {
        for (K vertexId : temporaryGraphCache.getAllEvolveVId()) {
            ITraversalRequest<K> traversalRequest = new VertexBeginTraversalRequest<>(vertexId);
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.incVcTraversalFunction.init(traversalRequest);
        }
    }

    @Override
    protected void traversalByRequest() {
        if (graphVCTraversalCtx.isEnableIncrMatch() && DynamicGraphHelper.enableIncrTraversalRuntime(runtimeContext)) {
            traversalEvolveVIds();

        } else {
            if (function.getMaxIterationCount() <= 2) {
                // The evolved vertices/edges can cover the match pattern when iteration <= 2 (e.g match(a)->(b)).
                traversalEvolveVIds();
                return;
            }

            // Traversal all vertices.
            if (!temporaryGraphCache.getAllEvolveVId().isEmpty()) {
                try (CloseableIterator<K> idIterator =
                         graphState.dynamicGraph().V().query(GRAPH_VERSION, keyGroup).idIterator()) {
                    while (idIterator.hasNext()) {
                        K vertexId = idIterator.next();
                        ITraversalRequest<K> traversalRequest = new VertexBeginTraversalRequest<>(vertexId);
                        this.graphVCTraversalCtx.init(iterations, vertexId);
                        this.incVcTraversalFunction.init(traversalRequest);
                    }
                }
            }
        }
    }
}
