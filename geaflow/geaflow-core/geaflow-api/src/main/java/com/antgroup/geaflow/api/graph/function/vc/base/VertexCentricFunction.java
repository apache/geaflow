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

package com.antgroup.geaflow.api.graph.function.vc.base;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.Function;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.List;

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
