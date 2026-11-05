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
 */
package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/**
 * Compact adjacency index shared by the concrete and versioned graph providers.
 *
 * <p>Each vertex maps to a small {@code int[]} of positions in one immutable edge array, so a
 * neighbour list is a derived view rather than a per-vertex copy. That keeps memory proportional
 * to edges instead of edges plus boxed references, and it lets a high degree vertex expose its
 * adjacency without copying or sorting it.
 *
 * <p>An overlay only re-indexes the vertices a delta actually touches; every other vertex keeps
 * reading its parent indexes. This is what makes an incremental view cost local work instead of a
 * full rebuild.
 */
final class GraphNeighborIndex {

    private GraphNeighborIndex() {
    }

    /** Builds a position index over an immutable edge list. */
    static Map<String, int[]> indexes(List<GraphEdgeRef> edges) {
        Map<String, List<Integer>> mutable = new HashMap<>();
        for (int index = 0; index < edges.size(); index++) {
            GraphEdgeRef edge = edges.get(index);
            add(mutable, edge.getSourceEntityId(), index);
            if (!Objects.equals(edge.getSourceEntityId(), edge.getTargetEntityId())) {
                add(mutable, edge.getTargetEntityId(), index);
            }
        }
        Map<String, int[]> result = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : mutable.entrySet()) {
            result.put(entry.getKey(), toArray(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Re-indexes only the vertices affected by a delta and reuses the parent index elsewhere.
     *
     * <p>The affected set is the endpoints of deleted or replaced edges, the endpoints of added
     * edges, and every endpoint of a tombstoned entity's adjacency.
     */
    static Map<String, int[]> overlay(VersionedGraphView parent, GraphDelta delta,
                                      List<GraphEdgeRef> mergedEdges) {
        Set<String> changed = new LinkedHashSet<>();
        for (String edgeId : delta.getTombstones().getEdges()) {
            addEndpoints(parent.edgeById(edgeId), changed);
        }
        for (String entityId : delta.getTombstones().getEntities()) {
            changed.add(entityId);
            for (GraphEdgeRef edge : parent.neighbors(entityId)) {
                addEndpoints(edge, changed);
            }
        }
        for (GraphEdgeRef edge : delta.getEdges()) {
            addEndpoints(parent.edgeById(edge.getEdgeId()), changed);
            addEndpoints(edge, changed);
        }

        if (changed.isEmpty()) {
            return parent.neighborIndexes();
        }
        // Only touched vertices are re-indexed against the child edge array. Untouched vertices
        // keep delegating to the parent view, whose positions index the parent edge array.
        Map<String, int[]> overlaid = new HashMap<>();
        Map<String, List<Integer>> local = new HashMap<>();
        for (int index = 0; index < mergedEdges.size(); index++) {
            GraphEdgeRef edge = mergedEdges.get(index);
            addIfTouched(local, changed, edge.getSourceEntityId(), index);
            addIfTouched(local, changed, edge.getTargetEntityId(), index);
        }
        for (String entityId : changed) {
            // A touched vertex is authoritative for this view even when it has no edges left:
            // an empty array must shadow the parent adjacency instead of delegating to it.
            List<Integer> positions = local.get(entityId);
            overlaid.put(entityId, positions == null ? EMPTY : toArray(positions));
        }
        return Collections.unmodifiableMap(overlaid);
    }

    private static final int[] EMPTY = new int[0];

    private static void addIfTouched(Map<String, List<Integer>> target, Set<String> changed,
                                     String entityId, int position) {
        if (changed.contains(entityId)) {
            add(target, entityId, position);
        }
    }

    private static int[] toArray(List<Integer> values) {
        int[] positions = new int[values.size()];
        for (int index = 0; index < positions.length; index++) {
            positions[index] = values.get(index);
        }
        return positions;
    }

    /** Returns a lazy, random-access view of the adjacency for one vertex. */
    static List<GraphEdgeRef> view(int[] indexes, List<GraphEdgeRef> edges) {
        return new NeighborView(indexes, edges);
    }

    private static void add(Map<String, List<Integer>> target, String key, int position) {
        target.computeIfAbsent(key, ignored -> new ArrayList<>()).add(position);
    }

    private static void addEndpoints(GraphEdgeRef edge, Set<String> target) {
        if (edge == null) {
            return;
        }
        target.add(edge.getSourceEntityId());
        target.add(edge.getTargetEntityId());
    }

    /** Random-access adjacency view that resolves edge positions on demand. */
    private static final class NeighborView extends AbstractList<GraphEdgeRef>
        implements RandomAccess {
        private final int[] indexes;
        private final List<GraphEdgeRef> edges;

        private NeighborView(int[] indexes, List<GraphEdgeRef> edges) {
            this.indexes = indexes;
            this.edges = edges;
        }

        @Override
        public GraphEdgeRef get(int index) {
            return edges.get(indexes[index]);
        }

        @Override
        public int size() {
            return indexes.length;
        }
    }

}