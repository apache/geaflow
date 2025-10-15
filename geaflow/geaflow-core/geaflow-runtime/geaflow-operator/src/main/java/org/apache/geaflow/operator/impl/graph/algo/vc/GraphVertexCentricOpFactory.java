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

import java.util.List;
import org.apache.geaflow.api.graph.compute.IncVertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.DynamicGraphVertexCentricComputeOp;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.DynamicGraphVertexCentricComputeWithAggOp;
import org.apache.geaflow.operator.impl.graph.compute.statical.StaticGraphVertexCentricComputeOp;
import org.apache.geaflow.operator.impl.graph.compute.statical.StaticGraphVertexCentricComputeWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalAllOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalAllWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByIdsOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByIdsWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByStreamOp;
import org.apache.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByStreamWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalAllOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalAllWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByIdsOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByIdsWithAggOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByStreamOp;
import org.apache.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByStreamWithAggOp;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class GraphVertexCentricOpFactory {

    public static <K, VV, EV, M> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricComputeOp(
        GraphViewDesc graphViewDesc,
        VertexCentricCompute<K, VV, EV, M> vertexCentricCompute) {
        return new StaticGraphVertexCentricComputeOp<>(graphViewDesc, vertexCentricCompute);
    }


    public static <K, VV, EV, M, I, PA, PR, GA, R> IGraphVertexCentricAggOp<K, VV, EV, M, I, PA, PR, R> buildStaticGraphVertexCentricAggComputeOp(
        GraphViewDesc graphViewDesc,
        VertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, R> vertexCentricAggCompute) {
        return new StaticGraphVertexCentricComputeWithAggOp<>(graphViewDesc, vertexCentricAggCompute);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalStartByStreamOp<>(graphViewDesc,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricAggTraversalOp(
        GraphViewDesc graphViewDesc,
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalStartByStreamWithAggOp<>(graphViewDesc,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalAllOp<>(graphViewDesc, vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricAggTraversalAllOp(
        GraphViewDesc graphViewDesc,
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalAllWithAggOp<>(graphViewDesc, vertexCentricTraversal);
    }


    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new StaticGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequest,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricAggTraversalOp(
        GraphViewDesc graphViewDesc,
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new StaticGraphVertexCentricTraversalStartByIdsWithAggOp<>(graphViewDesc, traversalRequest,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new StaticGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequests,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricAggTraversalOp(
        GraphViewDesc graphViewDesc,
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new StaticGraphVertexCentricTraversalStartByIdsWithAggOp<>(graphViewDesc, traversalRequests,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricComputeOp(
        GraphViewDesc graphViewDesc, IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute) {
        return new DynamicGraphVertexCentricComputeOp(graphViewDesc, incVertexCentricCompute);
    }

    public static <K, VV, EV, M, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricAggComputeOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, GR> incVertexCentricCompute) {
        return new DynamicGraphVertexCentricComputeWithAggOp(graphViewDesc, incVertexCentricCompute);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new DynamicGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequest,
            incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new DynamicGraphVertexCentricTraversalStartByIdsWithAggOp<>(graphViewDesc, traversalRequest,
            incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalAllOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalAllWithAggOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalStartByStreamOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalStartByStreamWithAggOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new DynamicGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc,
            traversalRequests,
            incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R, I, PA, PR, GA, GR> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new DynamicGraphVertexCentricTraversalStartByIdsWithAggOp<>(graphViewDesc,
            traversalRequests,
            incVertexCentricTraversal);
    }

}
