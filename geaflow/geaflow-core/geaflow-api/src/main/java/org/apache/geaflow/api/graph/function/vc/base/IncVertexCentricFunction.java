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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.Function;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction.VertexQuery;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.pushdown.filter.IVertexFilter;

/**
 * Interface for incremental vertex centric compute function.
 *
 * @param <K>  The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M>  The message type during iterations.
 */
public interface IncVertexCentricFunction<K, VV, EV, M> extends Function {

    /**
     * Evolve based on temporary graph in first iteration.
     *
     * @param vertexId       The vertex id.
     * @param temporaryGraph The incremental memory graph.
     */
    void evolve(K vertexId, TemporaryGraph<K, VV, EV> temporaryGraph);

    /**
     * Perform computing based on message iterator during iterations.
     */
    void compute(K vertexId, Iterator<M> messageIterator);

    /**
     * Finish iteration computation, could add vertices and edges from temporaryGraph into mutableGraph.
     */
    void finish(K vertexId, MutableGraph<K, VV, EV> mutableGraph);

    interface IncGraphContext<K, VV, EV, M> {

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
         * Returns the mutable graph.
         */
        MutableGraph<K, VV, EV> getMutableGraph();

        /**
         * Returns the incremental graph.
         */
        TemporaryGraph<K, VV, EV> getTemporaryGraph();

        /**
         * Returns the historical graph on graph state.
         */
        HistoricalGraph<K, VV, EV> getHistoricalGraph();

        /**
         * Send message to vertex.
         */
        void sendMessage(K vertexId, M message);

        /**
         * Send message to neighbors of current vertex.
         */
        void sendMessageToNeighbors(M message);

    }

    interface TemporaryGraph<K, VV, EV> {

        /**
         * Returns the current vertex.
         */
        IVertex<K, VV> getVertex();

        /**
         * Returns the edges of current vertex.
         */
        List<IEdge<K, EV>> getEdges();

        /**
         * Update value of current vertex.
         */
        void updateVertexValue(VV value);

    }

    interface HistoricalGraph<K, VV, EV> {

        /**
         * Returns the latest version id of graph state.
         */
        Long getLatestVersionId();

        /**
         * Returns all version ids of graph state.
         */
        List<Long> getAllVersionIds();

        /**
         * Returns all vertices of all versions.
         */
        Map<Long, IVertex<K, VV>> getAllVertex();

        /**
         * Get all vertices of specified versions.
         */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions);

        /**
         * Get all vertices of specified versions which satisfy the filter.
         */
        Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions, IVertexFilter<K, VV> vertexFilter);

        /**
         * Get the graph snapshot of specified version.
         */
        GraphSnapShot<K, VV, EV> getSnapShot(long version);

    }

    interface GraphSnapShot<K, VV, EV> {

        /**
         * Returns the snapshot's version.
         */
        long getVersion();

        /**
         * Returns the VertexQuery.
         */
        VertexQuery<K, VV> vertex();

        /**
         * Returns the EdgeQuery.
         */
        EdgeQuery<K, EV> edges();

    }

    interface MutableGraph<K, VV, EV> {

        /**
         * Add vertex into mutable graph with specified version.
         */
        void addVertex(long version, IVertex<K, VV> vertex);

        /**
         * Add edge into mutable graph with specified version.
         */
        void addEdge(long version, IEdge<K, EV> edge);

    }


}
