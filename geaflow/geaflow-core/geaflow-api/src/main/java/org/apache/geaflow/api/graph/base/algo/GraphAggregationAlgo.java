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

package org.apache.geaflow.api.graph.base.algo;

import org.apache.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;

/**
 * Interface for graph aggregation algo function.
 *
 * @param <I>  The type of aggregate input iterm.
 * @param <PA> The type of partial aggregate iterm.
 * @param <PR> The type of partial aggregate result.
 * @param <GA> The type of global aggregate iterm.
 * @param <GR> The type of global aggregate result.
 */
public interface GraphAggregationAlgo<I, PA, PR, GA, GR> {

    VertexCentricAggregateFunction<I, PA, PR, GA, GR> getAggregateFunction();
}
