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

import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import org.apache.geaflow.api.graph.function.vc.base.IncVertexCentricFunction;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;

/**
 * Interface for incremental vertex centric traversal function.
 *
 * @param <K>  The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M>  The message type during iterations.
 * @param <R>  The request type for traversal.
 */
public interface IncVertexCentricTraversalFunction<K, VV, EV, M, R> extends IncVertexCentricFunction<K, VV
    , EV, M> {

    /**
     * Open incremental traversal function based on context.
     */
    void open(IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext);

    /**
     * Initialize the traversal by request.
     */
    void init(ITraversalRequest<K> traversalRequest);

    /**
     * Finish iteration traversal.
     */
    void finish();

    /**
     * Close resources in iteration traversal.
     */
    void close();

    interface IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> extends IncGraphContext<K, VV, EV,
        M> {

        /**
         * Active traversal request to process.
         */
        void activeRequest(ITraversalRequest<K> request);

        /**
         * Receive the response.
         */
        void takeResponse(ITraversalResponse<R> response);

        /**
         * Broadcast message.
         */
        void broadcast(IGraphMessage<K, M> message);

        /**
         * Get the historical graph of graph state.
         */
        TraversalHistoricalGraph<K, VV, EV> getHistoricalGraph();

        /**
         * Get the traversal operator name.
         */
        String getTraversalOpName();
    }


    interface TraversalHistoricalGraph<K, VV, EV> extends HistoricalGraph<K, VV, EV> {

        /**
         * Get the graph snapshot of specified version.
         */
        TraversalGraphSnapShot<K, VV, EV> getSnapShot(long version);
    }

    interface TraversalGraphSnapShot<K, VV, EV> extends GraphSnapShot<K, VV, EV> {

        /**
         * Returns the TraversalVertexQuery.
         */
        TraversalVertexQuery<K, VV> vertex();

        /**
         * Returns the TraversalEdgeQuery.
         */
        TraversalEdgeQuery<K, EV> edges();
    }
}
