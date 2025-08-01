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

package org.apache.geaflow.api.graph.traversal;

import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;

public abstract class IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR>
    extends AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R,
    IncVertexCentricAggTraversalFunction<K, VV, EV, M, R, I, GR>>
    implements GraphAggregationAlgo<I, PA, PR, GA, GR> {

    public IncVertexCentricAggTraversal(long iterations) {
        super(iterations);
    }

    public IncVertexCentricAggTraversal(long iterations, String name) {
        super(iterations, name);
    }
}
