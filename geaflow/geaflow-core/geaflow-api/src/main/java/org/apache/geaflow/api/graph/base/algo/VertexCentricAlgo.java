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

import org.apache.geaflow.api.function.iterator.IteratorFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.partition.graph.edge.IGraphVCPartition;
import org.apache.geaflow.common.encoder.IEncoder;

public abstract class VertexCentricAlgo<K, VV, EV, M> implements IteratorFunction {

    protected String name;
    protected long iterations;

    protected VertexCentricAlgo(long iterations) {
        this.iterations = iterations;
        this.name = this.getClass().getSimpleName();
    }

    protected VertexCentricAlgo(long iterations, String name) {
        this.iterations = iterations;
        this.name = name;
    }

    /**
     * Returns vertex centric combine function.
     */
    public abstract VertexCentricCombineFunction<M> getCombineFunction();

    public IEncoder<K> getKeyEncoder() {
        return null;
    }

    public IEncoder<M> getMessageEncoder() {
        return null;
    }

    public IGraphVCPartition<K> getGraphPartition() {
        return null;
    }

    @Override
    public final long getMaxIterationCount() {
        return this.iterations;
    }

    public String getName() {
        return this.name;
    }
}
