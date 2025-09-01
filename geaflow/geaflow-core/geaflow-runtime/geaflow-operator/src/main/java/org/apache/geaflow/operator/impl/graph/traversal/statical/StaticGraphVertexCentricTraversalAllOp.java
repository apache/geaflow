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
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class StaticGraphVertexCentricTraversalAllOp<K, VV, EV, M, R,
    FUNC extends VertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractStaticGraphVertexCentricTraversalOp<K, VV, EV, M, R, FUNC> {

    public StaticGraphVertexCentricTraversalAllOp(GraphViewDesc graphViewDesc,
                                                  AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
    }

    @Override
    protected void traversalByRequest(long iterations) {
        try (CloseableIterator<K> idIterator = graphVCTraversalCtx.vertex().loadIdIterator()) {
            while (idIterator.hasNext()) {
                K vertexId = idIterator.next();
                ITraversalRequest<K> traversalRequest = new VertexBeginTraversalRequest(vertexId);
                this.graphVCTraversalCtx.init(iterations, vertexId);
                this.vcTraversalFunction.init(traversalRequest);
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
