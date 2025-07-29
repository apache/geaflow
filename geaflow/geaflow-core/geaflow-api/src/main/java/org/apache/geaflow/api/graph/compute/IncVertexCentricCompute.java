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

package org.apache.geaflow.api.graph.compute;

import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricComputeAlgo;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;


public abstract class IncVertexCentricCompute<K, VV, EV, M>
    extends AbstractIncVertexCentricComputeAlgo<K, VV, EV, M, IncVertexCentricComputeFunction<K, VV, EV, M>> {

    public IncVertexCentricCompute(long iterations) {
        super(iterations);
    }

    /**
     * Returns incremental vertex centric compute function.
     */
    public abstract IncVertexCentricComputeFunction<K, VV, EV, M> getIncComputeFunction();

}
