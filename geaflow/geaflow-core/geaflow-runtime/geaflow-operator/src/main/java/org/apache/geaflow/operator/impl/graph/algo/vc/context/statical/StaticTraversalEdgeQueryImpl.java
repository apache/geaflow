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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.statical;

import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public class StaticTraversalEdgeQueryImpl<K, VV, EV> extends StaticEdgeQueryImpl<K, VV, EV>
    implements TraversalEdgeQuery<K, EV> {

    public StaticTraversalEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState) {
        super(vId, graphState);
    }

    public StaticTraversalEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState, KeyGroup keyGroup) {
        super(vId, graphState, keyGroup);
    }

    @Override
    public TraversalEdgeQuery<K, EV> withId(K vertexId) {
        this.vId = vertexId;
        return this;
    }
}
