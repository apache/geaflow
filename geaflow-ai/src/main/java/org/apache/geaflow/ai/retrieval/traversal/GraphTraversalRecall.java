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

package org.apache.geaflow.ai.retrieval.traversal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.ToDoubleFunction;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;

/** Read-only, budgeted graph traversal with deterministic weighted sampling and provenance. */
public final class GraphTraversalRecall {
    private static final Comparator<GraphVertex> VERTEX_ORDER = Comparator
        .comparing(GraphVertex::getLabel).thenComparing(v -> v.getVertex().getId());
    private static final Comparator<Transition> TRANSITION_ORDER = Comparator
        .comparing((Transition t) -> t.edge.getLabel())
        .thenComparing(t -> t.edge.getEdge().getSrcId())
        .thenComparing(t -> t.edge.getEdge().getDstId())
        .thenComparing(t -> t.target, VERTEX_ORDER)
        .thenComparing((left, right) -> compareValues(left.edge.getEdge().getValues(),
            right.edge.getEdge().getValues()))
        .thenComparingDouble(t -> t.weight);

    private static int compareValues(List<String> left, List<String> right) {
        for (int i = 0; i < Math.min(left.size(), right.size()); i++) {
            int order = Comparator.nullsFirst(Comparator.<String>naturalOrder()).compare(left.get(i), right.get(i));
            if (order != 0) {
                return order;
            }
        }
        return Integer.compare(left.size(), right.size());
    }

    public static final class Limits {
        public final int maxHops;
        public final int maxVertices;
        public final int maxScannedEdges;
        public final int maxTraversedEdges;
        public final int maxResults;
        public final int stepsPerAnchor;

        public Limits(int hops, int vertices, int edges, int results, int steps) {
            this(hops, vertices, edges, Integer.MAX_VALUE, results, steps);
        }

        public Limits(int hops, int vertices, int edges, int traversals, int results, int steps) {
            if (hops < 1 || vertices < 1 || edges < 1 || traversals < 1 || results < 1 || steps < 1) {
                throw new IllegalArgumentException("limits must be positive");
            }
            maxHops = hops;
            maxVertices = vertices;
            maxScannedEdges = edges;
            maxTraversedEdges = traversals;
            maxResults = results;
            stepsPerAnchor = steps;
        }
    }

    public enum StopReason { COMPLETED, VERTEX_LIMIT, EDGE_SCAN_LIMIT, EDGE_TRAVERSAL_LIMIT, DEADLINE, CANCELLED }

    public enum Strategy { RANDOM_WALK_RESTART, BREADTH_FIRST, DEPTH_FIRST }

    public static final class Hit {
        public final GraphVertex candidate;
        public final GraphVertex anchor;
        public final List<GraphEdge> path;
        public final int visits;
        public final double score;

        private Hit(GraphVertex vertex, Accumulator value, int steps) {
            candidate = vertex;
            anchor = value.anchor;
            path = Collections.unmodifiableList(new ArrayList<>(value.path));
            visits = value.visits;
            // Sum of per-anchor visit frequencies, not an exact stationary PPR probability.
            score = (double) visits / steps;
        }
    }

    public static final class Result {
        public final List<Hit> hits;
        public final StopReason stopReason;
        public final int scannedEdges;
        public final int traversedEdges;
        public final int visitedVertices;
        public final boolean hopLimitReached;
        public final boolean resultsTruncated;

        private Result(List<Hit> hits, Session session, boolean truncated) {
            this.hits = Collections.unmodifiableList(hits);
            stopReason = session.reason;
            scannedEdges = session.scanned;
            traversedEdges = session.traversed;
            visitedVertices = session.visited.size();
            hopLimitReached = session.hopLimitReached;
            resultsTruncated = truncated;
        }
    }

    public Result recall(GraphAccessor accessor, List<GraphVertex> anchors, Limits limits,
                         long seed, double restartProbability, ToDoubleFunction<GraphEdge> weights,
                         long deadlineNanos, LongSupplier clock, BooleanSupplier cancelled) {
        return recall(accessor, anchors, limits, Strategy.RANDOM_WALK_RESTART, seed,
            restartProbability, weights, deadlineNanos, clock, cancelled);
    }

    public Result recall(GraphAccessor accessor, List<GraphVertex> anchors, Limits limits,
                         Strategy strategy, long seed, double restartProbability,
                         ToDoubleFunction<GraphEdge> weights, long deadlineNanos,
                         LongSupplier clock, BooleanSupplier cancelled) {
        if (!Double.isFinite(restartProbability) || restartProbability < 0 || restartProbability > 1) {
            throw new IllegalArgumentException("restart probability must be in [0, 1]");
        }
        Session session = new Session(accessor, limits, weights, deadlineNanos, clock, cancelled);
        List<GraphVertex> orderedAnchors = new ArrayList<>(new HashSet<>(anchors));
        orderedAnchors.sort(VERTEX_ORDER);
        Set<GraphVertex> anchorSet = new HashSet<>(orderedAnchors);
        Random random = new Random(seed);
        Map<GraphVertex, Accumulator> counts = new HashMap<>();
        if (strategy != Strategy.RANDOM_WALK_RESTART) {
            for (GraphVertex anchor : orderedAnchors) {
                search(session, anchor, anchorSet, strategy, counts);
            }
            return result(counts, 1, session);
        }
        for (GraphVertex anchor : orderedAnchors) {
            if (!session.active() || !session.visit(anchor)) {
                break;
            }
            GraphVertex current = anchor;
            List<GraphEdge> path = new ArrayList<>();
            for (int step = 0; step < limits.stepsPerAnchor && session.active(); step++) {
                if (path.size() >= limits.maxHops) {
                    session.hopLimitReached = true;
                    current = anchor;
                    path.clear();
                    continue;
                }
                if (random.nextDouble() < restartProbability) {
                    current = anchor;
                    path.clear();
                    continue;
                }
                WeightedNeighbors transitions = session.neighbors(current);
                if (!session.active()) {
                    break;
                }
                if (transitions.values.isEmpty()) {
                    current = anchor;
                    path.clear();
                    continue;
                }
                Transition selected = transitions.select(random.nextDouble());
                if (!session.traverse(selected.target)) {
                    break;
                }
                path.add(selected.edge);
                current = selected.target;
                if (!anchorSet.contains(current)) {
                    Accumulator value = counts.computeIfAbsent(current, ignored -> new Accumulator());
                    value.visits++;
                    // Shortest observed walk; global shortest-path search is not performed.
                    if (value.path == null || path.size() < value.path.size()) {
                        value.path = new ArrayList<>(path);
                        value.anchor = anchor;
                    }
                }
            }
        }
        return result(counts, limits.stepsPerAnchor, session);
    }

    private static Result result(Map<GraphVertex, Accumulator> counts, int denominator, Session session) {
        List<Hit> hits = new ArrayList<>();
        for (Map.Entry<GraphVertex, Accumulator> entry : counts.entrySet()) {
            hits.add(new Hit(entry.getKey(), entry.getValue(), denominator));
        }
        hits.sort(Comparator.comparingDouble((Hit h) -> h.score).reversed()
            .thenComparing(h -> h.candidate, VERTEX_ORDER));
        return new Result(new ArrayList<>(hits.subList(0, Math.min(hits.size(), session.limits.maxResults))),
            session, hits.size() > session.limits.maxResults);
    }

    private static void search(Session session, GraphVertex anchor, Set<GraphVertex> anchors,
                               Strategy strategy, Map<GraphVertex, Accumulator> counts) {
        if (!session.active() || !session.visit(anchor)) {
            return;
        }
        ArrayDeque<SearchState> frontier = new ArrayDeque<>();
        frontier.add(new SearchState(anchor, Collections.emptyList()));
        // A vertex found at a smaller depth must be expanded again under depth-limited DFS.
        Map<GraphVertex, Integer> depths = new HashMap<>();
        depths.put(anchor, 0);
        Set<GraphVertex> counted = new HashSet<>();
        while (session.active() && !frontier.isEmpty()) {
            SearchState state = strategy == Strategy.BREADTH_FIRST
                ? frontier.removeFirst() : frontier.removeLast();
            if (state.path.size() >= session.limits.maxHops) {
                session.hopLimitReached = true;
                continue;
            }
            if (state.path.size() > depths.get(state.vertex)) {
                continue;
            }
            List<Transition> neighbors = session.neighbors(state.vertex).values;
            if (!session.active()) {
                break;
            }
            for (Transition next : neighbors) {
                if (!session.active() || !session.traverse(next.target)) {
                    break;
                }
                int depth = state.path.size() + 1;
                if (depth >= depths.getOrDefault(next.target, Integer.MAX_VALUE)) {
                    continue;
                }
                depths.put(next.target, depth);
                List<GraphEdge> path = new ArrayList<>(state.path);
                path.add(next.edge);
                frontier.addLast(new SearchState(next.target, path));
                if (!anchors.contains(next.target)) {
                    Accumulator value = counts.computeIfAbsent(next.target, ignored -> new Accumulator());
                    if (counted.add(next.target)) {
                        value.visits++;
                    }
                    if (value.path == null || depth < value.path.size()) {
                        value.path = path;
                        value.anchor = anchor;
                    }
                }
            }
        }
    }

    private static final class SearchState {
        private final GraphVertex vertex;
        private final List<GraphEdge> path;

        private SearchState(GraphVertex vertex, List<GraphEdge> path) {
            this.vertex = vertex;
            this.path = path;
        }
    }

    private static final class Accumulator {
        private GraphVertex anchor;
        private List<GraphEdge> path;
        private int visits;
    }

    /** Builds a weight CDF once per adjacency; each repeated draw costs O(log degree). */
    private static final class WeightedNeighbors {
        private final List<Transition> values;
        private final double[] cumulative;

        private WeightedNeighbors(List<Transition> values) {
            this.values = values;
            cumulative = new double[values.size()];
            double max = 0;
            for (Transition value : values) {
                max = Math.max(max, value.weight);
            }
            double sum = 0;
            for (int i = 0; i < values.size(); i++) {
                // Scaling prevents overflow when several finite weights approach Double.MAX_VALUE.
                sum += values.get(i).weight / max;
                cumulative[i] = sum;
            }
        }

        private Transition select(double uniform) {
            double draw = uniform * cumulative[cumulative.length - 1];
            int low = 0;
            int high = cumulative.length - 1;
            while (low < high) {
                int mid = low + (high - low) / 2;
                if (cumulative[mid] <= draw) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return values.get(low);
        }
    }

    private static final class Transition {
        private final GraphEdge edge;
        private final GraphVertex target;
        private final double weight;

        private Transition(GraphEdge edge, GraphVertex target, double weight) {
            this.edge = edge;
            this.target = target;
            this.weight = weight;
        }
    }

    private static final class Session {
        private final GraphAccessor accessor;
        private final Limits limits;
        private final ToDoubleFunction<GraphEdge> weights;
        private final long deadline;
        private final LongSupplier clock;
        private final BooleanSupplier cancelled;
        private final Set<GraphVertex> visited = new HashSet<>();
        private final Map<GraphVertex, WeightedNeighbors> adjacency = new LinkedHashMap<>();
        private StopReason reason = StopReason.COMPLETED;
        private int scanned;
        private int traversed;
        private boolean hopLimitReached;

        private Session(GraphAccessor accessor, Limits limits, ToDoubleFunction<GraphEdge> weights,
                        long deadline, LongSupplier clock, BooleanSupplier cancelled) {
            this.accessor = accessor;
            this.limits = limits;
            this.weights = weights;
            this.deadline = deadline;
            this.clock = clock;
            this.cancelled = cancelled;
        }

        private boolean active() {
            if (reason != StopReason.COMPLETED) {
                return false;
            }
            if (cancelled.getAsBoolean()) {
                reason = StopReason.CANCELLED;
            } else if (deadline != 0 && clock.getAsLong() - deadline >= 0) {
                reason = StopReason.DEADLINE;
            }
            return reason == StopReason.COMPLETED;
        }

        private boolean visit(GraphVertex vertex) {
            if (!visited.contains(vertex) && visited.size() >= limits.maxVertices) {
                reason = StopReason.VERTEX_LIMIT;
                return false;
            }
            visited.add(vertex);
            return true;
        }

        private boolean traverse(GraphVertex vertex) {
            if (traversed >= limits.maxTraversedEdges) {
                reason = StopReason.EDGE_TRAVERSAL_LIMIT;
                return false;
            }
            if (!visit(vertex)) {
                return false;
            }
            traversed++;
            return true;
        }

        private WeightedNeighbors neighbors(GraphVertex vertex) {
            if (adjacency.containsKey(vertex)) {
                return adjacency.get(vertex);
            }
            List<Transition> result = new ArrayList<>();
            Set<Edge> selfLoops = Collections.newSetFromMap(new IdentityHashMap<>());
            Iterator<GraphEdge> edges = accessor.scanEdge(vertex);
            while (active() && edges.hasNext()) {
                if (scanned >= limits.maxScannedEdges) {
                    reason = StopReason.EDGE_SCAN_LIMIT;
                    break;
                }
                GraphEdge edge = edges.next();
                scanned++;
                // The local accessor can emit the same backing self-loop through both indexes.
                // Identity preserves distinct parallel edges, whose equals() ignores properties.
                boolean selfLoop = edge.getEdge().getSrcId().equals(edge.getEdge().getDstId());
                if (selfLoop && !selfLoops.add(edge.getEdge())) {
                    continue;
                }
                double weight = weights.applyAsDouble(edge);
                if (!Double.isFinite(weight) || weight < 0) {
                    throw new IllegalArgumentException("edge weight must be finite and non-negative");
                }
                if (weight == 0) {
                    continue;
                }
                Set<GraphVertex> endpoints = new HashSet<>();
                for (GraphEntity endpoint : accessor.expand(edge)) {
                    if (!active()) {
                        break;
                    }
                    if (endpoint instanceof GraphVertex && endpoints.add((GraphVertex) endpoint)
                        && (selfLoop || !endpoint.equals(vertex))) {
                        result.add(new Transition(edge, (GraphVertex) endpoint, weight));
                    }
                }
            }
            result.sort(TRANSITION_ORDER);
            WeightedNeighbors neighbors = new WeightedNeighbors(result);
            adjacency.put(vertex, neighbors);
            return neighbors;
        }
    }
}
