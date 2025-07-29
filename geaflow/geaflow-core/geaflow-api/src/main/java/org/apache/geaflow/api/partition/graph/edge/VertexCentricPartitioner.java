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

package org.apache.geaflow.api.partition.graph.edge;

import org.apache.geaflow.api.partition.IPartition;
import org.apache.geaflow.api.partition.graph.IGraphPartitioner;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class VertexCentricPartitioner<K, VV, EV> implements IGraphPartitioner<K, VV, EV,
    IVertex<K, VV>, IEdge<K, EV>> {

    @Override
    public IPartition getVertexPartitioner() {
        return new DefaultVertexPartition();
    }

    static class DefaultVertexPartition<K, VV> implements IPartition<IVertex<K, VV>> {

        @Override
        public int[] partition(IVertex<K, VV> value, int numPartition) {
            return new int[]{Math.abs(value.getId().hashCode() % numPartition)};
        }
    }

    @Override
    public IPartition getEdgePartitioner() {
        return new DefaultEdgePartition();
    }

    static class DefaultEdgePartition<K, EV> implements IPartition<IEdge<K, EV>> {

        @Override
        public int[] partition(IEdge<K, EV> value, int numPartition) {
            return new int[]{Math.abs(value.getSrcId().hashCode() % numPartition)};
        }
    }

}
