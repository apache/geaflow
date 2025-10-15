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

package org.apache.geaflow.api.graph.function.vc;

import java.util.Iterator;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction;

/**
 * Interface for vertex centric compute function.
 *
 * @param <K>  The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M>  The message type during iterations.
 */
public interface VertexCentricComputeFunction<K, VV, EV, M> extends VertexCentricFunction<K, VV, EV, M> {

    /**
     * Initialize compute function based on context.
     */
    void init(VertexCentricComputeFuncContext<K, VV, EV, M> vertexCentricFuncContext);

    /**
     * Perform traversing based on message iterator during iterations.
     */
    void compute(K vertexId, Iterator<M> messageIterator);

    /**
     * Finish iteration computation.
     */
    void finish();

    interface VertexCentricComputeFuncContext<K, VV, EV, M> extends VertexCentricFuncContext<K, VV, EV, M> {

        /**
         * Update new value of current vertex.
         */
        void setNewVertexValue(VV value);

    }

}
