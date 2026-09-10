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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Function;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;

/**
 * Storage-independent seeded one-hop neighbor sampling.
 * The supplied ID encoder must return a stable, canonical byte representation across workers.
 */
public final class DeterministicNeighborSampler {

    public static final long DEFAULT_MAX_RETURNED_EDGES = 100000L;

    private DeterministicNeighborSampler() {
    }

    public static <K, EV> List<IEdge<K, EV>> sample(K vertexId,
                                                    Iterable<? extends IEdge<K, EV>> edges,
                                                    EdgeDirection direction,
                                                    int fanout,
                                                    Comparator<K> idComparator,
                                                    Function<? super K, byte[]> idEncoder) {
        return sample(vertexId, edges, direction, fanout, idComparator,
            DEFAULT_MAX_RETURNED_EDGES, 0L, 0L, idEncoder);
    }

    public static <K, EV> List<IEdge<K, EV>> sample(K vertexId,
                                                    Iterable<? extends IEdge<K, EV>> edges,
                                                    EdgeDirection direction,
                                                    int fanout,
                                                    Comparator<K> idComparator,
                                                    long maxReturnedEdges,
                                                    long seed,
                                                    long samplingVersion,
                                                    Function<? super K, byte[]> idEncoder) {
        return select(vertexId, edges, direction, fanout, idComparator, maxReturnedEdges,
            seed, samplingVersion, idEncoder, true);
    }

    /** Project an already direction-filtered local neighborhood to a smaller fanout. */
    public static <K, EV> List<IEdge<K, EV>> project(K vertexId,
                                                      Iterable<? extends IEdge<K, EV>> edges,
                                                      EdgeDirection direction,
                                                      int fanout,
                                                      Comparator<K> idComparator,
                                                      Function<? super K, byte[]> idEncoder) {
        return project(vertexId, edges, direction, fanout, idComparator,
            DEFAULT_MAX_RETURNED_EDGES, 0L, 0L, idEncoder);
    }

    public static <K, EV> List<IEdge<K, EV>> project(K vertexId,
                                                      Iterable<? extends IEdge<K, EV>> edges,
                                                      EdgeDirection direction,
                                                      int fanout,
                                                      Comparator<K> idComparator,
                                                      long maxReturnedEdges,
                                                      long seed,
                                                      long samplingVersion,
                                                      Function<? super K, byte[]> idEncoder) {
        return select(vertexId, edges, direction, fanout, idComparator, maxReturnedEdges,
            seed, samplingVersion, idEncoder, false);
    }

    private static <K, EV> List<IEdge<K, EV>> select(K vertexId,
                                                      Iterable<? extends IEdge<K, EV>> edges,
                                                      EdgeDirection direction,
                                                      int fanout,
                                                      Comparator<K> idComparator,
                                                      long maxReturnedEdges,
                                                      long seed,
                                                      long samplingVersion,
                                                      Function<? super K, byte[]> idEncoder,
                                                      boolean filterAndNormalize) {
        validate(vertexId, edges, direction, fanout, idComparator, maxReturnedEdges, idEncoder);
        long vertexHash = hashId(vertexId, idEncoder);
        Comparator<NeighborGroup<K, EV>> rankComparator = (left, right) -> {
            int result = Long.compareUnsigned(left.score, right.score);
            return result != 0 ? result
                : compareIds(left.neighborId, right.neighborId, idComparator, idEncoder);
        };
        Map<K, NeighborGroup<K, EV>> selected = new HashMap<>();
        PriorityQueue<NeighborGroup<K, EV>> worstFirst =
            fanout < 0 ? null : new PriorityQueue<>(fanout, rankComparator.reversed());

        for (IEdge<K, EV> sourceEdge : edges) {
            Objects.requireNonNull(sourceEdge, "edge");
            if (filterAndNormalize && !matchesDirection(sourceEdge, direction)) {
                continue;
            }
            IEdge<K, EV> edge = filterAndNormalize ? normalize(sourceEdge) : sourceEdge;
            K neighborId = neighborId(vertexId, edge);
            if (neighborId == null) {
                continue;
            }
            NeighborGroup<K, EV> group = selected.get(neighborId);
            if (group != null) {
                group.edges.add(edge);
                continue;
            }

            group = new NeighborGroup<>(neighborId,
                sampleScore(seed, samplingVersion, vertexHash, direction,
                    hashId(neighborId, idEncoder)), edge);
            if (fanout < 0 || selected.size() < fanout) {
                selected.put(neighborId, group);
                if (worstFirst != null) {
                    worstFirst.add(group);
                }
            } else if (rankComparator.compare(group, worstFirst.peek()) < 0) {
                NeighborGroup<K, EV> removed = worstFirst.remove();
                selected.remove(removed.neighborId);
                selected.put(neighborId, group);
                worstFirst.add(group);
            }
        }

        List<NeighborGroup<K, EV>> groups = new ArrayList<>(selected.values());
        groups.sort(rankComparator);
        List<IEdge<K, EV>> result = new ArrayList<>();
        for (NeighborGroup<K, EV> group : groups) {
            group.edges.sort((left, right) -> compareEdges(left, right, idComparator, idEncoder));
            result.addAll(group.edges);
            if (result.size() > maxReturnedEdges) {
                throw new IllegalStateException(String.format(
                    "one-hop sampling edge limit exceeded, vertexId=%s, actual=%s, limit=%s",
                    vertexId, result.size(), maxReturnedEdges));
            }
        }
        return result;
    }

    private static void validate(Object vertexId, Iterable<?> edges, EdgeDirection direction,
                                 int fanout, Comparator<?> idComparator, long maxReturnedEdges,
                                 Function<?, byte[]> idEncoder) {
        Objects.requireNonNull(vertexId, "vertexId");
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(idComparator, "idComparator");
        Objects.requireNonNull(idEncoder, "idEncoder");
        if (fanout == 0 || fanout < -1) {
            throw new IllegalArgumentException("fanout must be -1 or greater than zero");
        }
        if (maxReturnedEdges < 1) {
            throw new IllegalArgumentException("maxReturnedEdges must be greater than zero");
        }
    }

    private static boolean matchesDirection(IEdge<?, ?> edge, EdgeDirection direction) {
        return direction == EdgeDirection.BOTH || edge.getDirect() == direction;
    }

    private static <K, EV> IEdge<K, EV> normalize(IEdge<K, EV> edge) {
        if (edge.getDirect() != EdgeDirection.IN) {
            return edge;
        }
        IEdge<K, EV> reversed = edge.reverse();
        // The reversed endpoints now represent the logical outgoing direction.
        reversed.setDirect(EdgeDirection.OUT);
        return reversed;
    }

    private static <K, EV> K neighborId(K vertexId, IEdge<K, EV> edge) {
        if (Objects.equals(vertexId, edge.getSrcId())) {
            return edge.getTargetId();
        }
        if (Objects.equals(vertexId, edge.getTargetId())) {
            return edge.getSrcId();
        }
        return null;
    }

    private static long sampleScore(long seed, long samplingVersion, long vertexHash,
                                    EdgeDirection direction, long neighborHash) {
        long value = mix64(seed) ^ Long.rotateLeft(mix64(samplingVersion), 11);
        value ^= Long.rotateLeft(vertexHash, 23);
        value ^= Long.rotateLeft(mix64(direction.ordinal()), 37);
        value ^= Long.rotateLeft(neighborHash, 47);
        return mix64(value);
    }

    private static <K> long hashId(K value, Function<? super K, byte[]> idEncoder) {
        return stableHash(Objects.requireNonNull(idEncoder.apply(value), "idEncoder result"));
    }

    private static long stableHash(byte[] value) {
        long hash = 0xcbf29ce484222325L;
        for (byte current : value) {
            hash ^= current & 0xffL;
            hash *= 0x100000001b3L;
        }
        return mix64(hash);
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static <K> int compareIds(K left, K right, Comparator<K> idComparator,
                                      Function<? super K, byte[]> idEncoder) {
        int result = idComparator.compare(left, right);
        return result != 0 ? result : compareBytes(
            Objects.requireNonNull(idEncoder.apply(left), "idEncoder result"),
            Objects.requireNonNull(idEncoder.apply(right), "idEncoder result"));
    }

    private static <K, EV> int compareEdges(IEdge<K, EV> left, IEdge<K, EV> right,
                                            Comparator<K> idComparator,
                                            Function<? super K, byte[]> idEncoder) {
        int result = compareIds(left.getSrcId(), right.getSrcId(), idComparator, idEncoder);
        if (result == 0) {
            result = compareIds(left.getTargetId(), right.getTargetId(), idComparator, idEncoder);
        }
        if (result == 0) {
            result = left.getDirect().compareTo(right.getDirect());
        }
        if (result == 0) {
            result = String.valueOf(labelOf(left)).compareTo(String.valueOf(labelOf(right)));
        }
        if (result == 0) {
            result = String.valueOf(timeOf(left)).compareTo(String.valueOf(timeOf(right)));
        }
        if (result == 0) {
            result = String.valueOf(left.getValue()).compareTo(String.valueOf(right.getValue()));
        }
        return result;
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int result = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static String labelOf(IEdge<?, ?> edge) {
        return edge instanceof IGraphElementWithLabelField
            ? ((IGraphElementWithLabelField) edge).getLabel() : null;
    }

    private static Long timeOf(IEdge<?, ?> edge) {
        return edge instanceof IGraphElementWithTimeField
            ? ((IGraphElementWithTimeField) edge).getTime() : null;
    }

    private static final class NeighborGroup<K, EV> {

        private final K neighborId;
        private final long score;
        private final List<IEdge<K, EV>> edges = new ArrayList<>();

        private NeighborGroup(K neighborId, long score, IEdge<K, EV> firstEdge) {
            this.neighborId = neighborId;
            this.score = score;
            this.edges.add(firstEdge);
        }
    }
}
