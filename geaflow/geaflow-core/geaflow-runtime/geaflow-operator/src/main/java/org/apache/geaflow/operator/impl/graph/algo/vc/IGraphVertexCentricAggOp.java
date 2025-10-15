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

package org.apache.geaflow.operator.impl.graph.algo.vc;

/**
 * Interface for graph vertex centric operator with aggregation.
 *
 * @param <K>  The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M>  The message type during iterations.
 * @param <I>  The type of aggregate input iterm.
 * @param <PA> The type of partial aggregate iterm.
 * @param <PR> The type of partial aggregate result.
 * @param <R>  The type of global aggregate result.
 */
public interface IGraphVertexCentricAggOp<K, VV, EV, M, I, PA, PR, R>
    extends IGraphVertexCentricOp<K, VV, EV, M>, IGraphAggregateOp<I, PA, PR, R> {

}
