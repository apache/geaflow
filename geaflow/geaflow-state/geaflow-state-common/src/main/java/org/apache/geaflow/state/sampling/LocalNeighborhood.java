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

package org.apache.geaflow.state.sampling;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

/**
 * Versioned one-hop state owned by a single vertex.
 */
public class LocalNeighborhood<K, VV, EV> implements Serializable {

    private final IVertex<K, VV> vertex;
    private final List<IEdge<K, EV>> edges;
    private final long snapshotVersion;
    private final long samplingVersion;

    public LocalNeighborhood(IVertex<K, VV> vertex, List<? extends IEdge<K, EV>> edges,
                             long snapshotVersion) {
        this(vertex, edges, snapshotVersion, snapshotVersion);
    }

    public LocalNeighborhood(IVertex<K, VV> vertex, List<? extends IEdge<K, EV>> edges,
                             long snapshotVersion, long samplingVersion) {
        this.vertex = Objects.requireNonNull(vertex, "vertex");
        this.edges = new ArrayList<>(Objects.requireNonNull(edges, "edges"));
        for (IEdge<K, EV> edge : this.edges) {
            Objects.requireNonNull(edge, "edge");
        }
        this.snapshotVersion = snapshotVersion;
        this.samplingVersion = samplingVersion;
    }

    public IVertex<K, VV> getVertex() {
        return vertex;
    }

    public List<IEdge<K, EV>> getEdges() {
        return Collections.unmodifiableList(edges);
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    public long getSamplingVersion() {
        return samplingVersion;
    }

    public boolean matches(long expectedSnapshotVersion, long expectedSamplingVersion) {
        return snapshotVersion == expectedSnapshotVersion
            && samplingVersion == expectedSamplingVersion;
    }

    public LocalNeighborhood<K, VV, EV> revalidate(IVertex<K, VV> currentVertex,
                                                    long currentSnapshotVersion) {
        if (currentSnapshotVersion < snapshotVersion) {
            throw new IllegalArgumentException("cannot revalidate a neighborhood to an older snapshot");
        }
        return new LocalNeighborhood<>(currentVertex, edges, currentSnapshotVersion, samplingVersion);
    }

    /**
     * Create a bounded view of this already direction-filtered neighborhood.
     */
    public LocalNeighborhood<K, VV, EV> project(EdgeDirection direction, int fanout,
                                                Comparator<K> idComparator,
                                                Function<? super K, byte[]> idEncoder) {
        return new LocalNeighborhood<>(vertex,
            DeterministicNeighborSampler.project(vertex.getId(), edges, direction, fanout,
                idComparator, idEncoder),
            snapshotVersion, samplingVersion);
    }

    public LocalNeighborhood<K, VV, EV> project(EdgeDirection direction, int fanout,
                                                Comparator<K> idComparator,
                                                long maxReturnedEdges,
                                                long seed,
                                                Function<? super K, byte[]> idEncoder) {
        return new LocalNeighborhood<>(vertex, DeterministicNeighborSampler.project(vertex.getId(),
            edges, direction, fanout, idComparator, maxReturnedEdges, seed, samplingVersion,
            idEncoder),
            snapshotVersion, samplingVersion);
    }
}
