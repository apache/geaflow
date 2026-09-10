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

package org.apache.geaflow.dsl.common.algo;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.apache.geaflow.api.graph.sampling.SubgraphSamplingSpec;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.sampling.DeterministicNeighborSampler;
import org.apache.geaflow.state.sampling.LocalNeighborhood;

/** Runtime-facing contract for reusable one-hop sampling. */
public interface AlgorithmSamplingRuntimeContext<K, M> extends AlgorithmRuntimeContext<K, M> {

    default LocalNeighborhood<Object, Row, Row> sampleOneHop(RowVertex vertex,
                                                              EdgeDirection direction,
                                                              int fanout) {
        return sampleOneHop(vertex, direction, fanout,
            DeterministicNeighborSampler.DEFAULT_MAX_RETURNED_EDGES, 0L,
            getSamplingSnapshotVersion());
    }

    default LocalNeighborhood<Object, Row, Row> sampleOneHop(RowVertex vertex,
                                                              EdgeDirection direction,
                                                              int fanout,
                                                              long maxReturnedEdges,
                                                              long seed,
                                                              long samplingVersion) {
        try (CloseableIterator<RowEdge> iterator = loadStaticEdgesIterator(direction)) {
            Iterable<RowEdge> edges = () -> iterator;
            @SuppressWarnings({"unchecked", "rawtypes"})
            IType<Object> idType = (IType) getGraphSchema().getIdType();
            Comparator<Object> comparator = idType::compare;
            Function<Object, byte[]> idEncoder = idType::serialize;
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<IEdge<Object, Row>> sampled = (List) DeterministicNeighborSampler.sample(
                vertex.getId(), edges, direction, fanout, comparator, maxReturnedEdges,
                seed, samplingVersion, idEncoder);
            return new LocalNeighborhood<>(vertex, sampled, getSamplingSnapshotVersion(),
                samplingVersion);
        }
    }

    default LocalNeighborhood<Object, Row, Row> sampleOneHop(RowVertex vertex,
                                                              SubgraphSamplingSpec spec,
                                                              long samplingVersion) {
        return sampleOneHop(vertex, spec.getDirection(), spec.getFanout(),
            spec.getMaxReturnedEdges(), spec.getSeed(), samplingVersion);
    }

    long getSamplingSnapshotVersion();

    default long getNeighborhoodChangeVersion(Object vertexId) {
        return Long.MIN_VALUE;
    }
}
