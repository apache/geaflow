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

package org.apache.geaflow.api.graph.function.vc.base;

import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.Function;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.pushdown.filter.IFilter;

public interface VertexCentricFunction<K, VV, EV, M> extends Function {

    interface VertexCentricFuncContext<K, VV, EV, M> {

        /**
         * Returns the job id.
         */
        long getJobId();

        /**
         * Returns the current iteration id.
         */
        long getIterationId();

        /**
         * Returns the runtime context.
         */
        RuntimeContext getRuntimeContext();

        /**
         * Returns the VertexQuery.
         */
        VertexQuery<K, VV> vertex();

        /**
         * Returns the EdgeQuery.
         */
        EdgeQuery<K, EV> edges();

        /**
         * Send message to vertex.
         */
        void sendMessage(K vertexId, M message);

        /**
         * Send message to neighbors of current vertex.
         */
        void sendMessageToNeighbors(M message);

    }

    interface VertexQuery<K, VV> {

        /**
         * Set vertex id.
         */
        VertexQuery<K, VV> withId(K vertexId);

        /**
         * Returns the current vertex.
         */
        IVertex<K, VV> get();

        /**
         * Get the vertex which satisfies filter condition.
         */
        IVertex<K, VV> get(IFilter vertexFilter);

    }

    interface EdgeQuery<K, EV> {

        /**
         * Returns the both edges.
         */
        List<IEdge<K, EV>> getEdges();

        /**
         * Returns the out edges.
         */
        List<IEdge<K, EV>> getOutEdges();

        /**
         * Returns the in edges.
         */
        List<IEdge<K, EV>> getInEdges();

        /**
         * Get the edges which satisfies filter condition.
         */
        CloseableIterator<IEdge<K, EV>> getEdges(IFilter<?> edgeFilter);
    }
}
