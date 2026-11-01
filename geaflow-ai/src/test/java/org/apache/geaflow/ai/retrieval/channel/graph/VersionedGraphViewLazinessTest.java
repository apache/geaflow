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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider.NeighborBatch;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the versioned graph view keeps adjacency lazy and bounds work before sorting.
 *
 * <p>A logical view that rebuilds every adjacency list at construction defeats the point of
 * incremental overlays: the design says a delta only touches the chunks, entities, edges and local
 * degrees it modifies. It also has to cap neighbour selection before ordering, otherwise a high
 * degree vertex pays a full sort to return a handful of edges.
 */
class VersionedGraphViewLazinessTest {
    private static final String GRAPH = "lazy-graph";
    private static final String VERSION = "v1";
    private static final int HIGH_DEGREE = 50_000;

    @Test
    void constructionDoesNotEnumerateAdjacencyLists() {
        CountingEdges edges = new CountingEdges(HIGH_DEGREE);
        GraphSnapshot snapshot = snapshotWithHub(edges);
        edges.iterations = 0;

        VersionedGraphView view = VersionedGraphView.from(snapshot);

        assertEquals(0, edges.iterations,
            "building a view must not walk every edge to materialise adjacency lists");
        assertEquals(HIGH_DEGREE, view.neighbors("hub").size());
        assertEquals(0, edges.iterations,
            "asking for a neighbour count must not walk the edge list either");
    }

    @Test
    void prioritisedSelectionBoundsSortingBeforeTruncation() {
        GraphSnapshot snapshot = snapshotWithHub(HIGH_DEGREE);
        VersionedGraphView view = VersionedGraphView.from(snapshot);

        NeighborBatch batch = view.prioritizedNeighbors("hub",
            Comparator.comparing(GraphEdgeRef::getEdgeId).reversed(), 3, 10);

        assertEquals(3, batch.getEdges().size());
        assertEquals(10, batch.getExamined(),
            "the scan must stop at the scan limit instead of ranking every neighbour");
        assertTrue(batch.hasMore());
    }

    @Test
    void overlayedViewKeepsLazyAdjacencyForUntouchedVertices() {
        GraphSnapshot snapshot = snapshotWithHub(HIGH_DEGREE);
        GraphDelta delta = GraphDelta.builder(new GraphVersion(GRAPH, "s2"),
                new GraphVersion(GRAPH, VERSION))
            .upsertEntity(entity("newcomer"))
            .build();

        VersionedGraphView view = VersionedGraphView.from(snapshot, delta);

        assertEquals(HIGH_DEGREE, view.neighbors("hub").size(),
            "untouched vertices must keep the parent adjacency size");
        assertTrue(view.neighbors("hub") instanceof java.util.RandomAccess);
    }

    @Test
    void overlayReflectsAddedEdgesForTheTouchedVertex() {
        GraphSnapshot snapshot = snapshotWithHub(4);
        GraphDelta delta = GraphDelta.builder(new GraphVersion(GRAPH, "s2"),
                new GraphVersion(GRAPH, VERSION))
            .upsertEntity(entity("newcomer"))
            .upsertEdge(new GraphEdgeRef("added-edge", "studies", "hub", "newcomer",
                Collections.singletonList("chunk-0")))
            .build();

        VersionedGraphView view = VersionedGraphView.from(snapshot, delta);

        List<String> edgeIds = new ArrayList<>();
        view.neighbors("hub").forEach(edge -> edgeIds.add(edge.getEdgeId()));
        assertTrue(edgeIds.contains("added-edge"),
            "a delta edge must appear in the touched vertex adjacency: " + edgeIds);
        assertEquals(5, view.neighbors("hub").size());
    }

    /** Edge list that records how often it is traversed, to expose eager materialisation. */
    private static final class CountingEdges extends AbstractList<GraphEdgeRef> {
        private final int degree;
        private int iterations;

        private CountingEdges(int degree) {
            this.degree = degree;
        }

        @Override
        public GraphEdgeRef get(int index) {
            iterations++;
            return new GraphEdgeRef("e" + index, "studies", "hub", "n" + index,
                Collections.singletonList("chunk-" + (index % 8)));
        }

        @Override
        public int size() {
            return degree;
        }

        @Override
        public Iterator<GraphEdgeRef> iterator() {
            return new Iterator<GraphEdgeRef>() {
                private int cursor;

                @Override
                public boolean hasNext() {
                    return cursor < degree;
                }

                @Override
                public GraphEdgeRef next() {
                    iterations++;
                    return get(cursor++);
                }
            };
        }
    }

    private static GraphSnapshot snapshotWithHub(CountingEdges edges) {
        List<EntityRef> entities = new ArrayList<>();
        entities.add(entity("hub"));
        for (int index = 0; index < edges.size(); index++) {
            entities.add(entity("n" + index));
        }
        return new GraphSnapshot(new GraphVersion(GRAPH, VERSION), entities, edges);
    }

    private static GraphSnapshot snapshotWithHub(int degree) {
        List<EntityRef> entities = new ArrayList<>();
        entities.add(entity("hub"));
        List<GraphEdgeRef> edges = new ArrayList<>();
        for (int index = 0; index < degree; index++) {
            String target = "n" + index;
            entities.add(entity(target));
            edges.add(new GraphEdgeRef("e" + index, "studies", "hub", target,
                Collections.singletonList("chunk-" + (index % 8))));
        }
        return new GraphSnapshot(new GraphVersion(GRAPH, VERSION), entities, edges);
    }

    private static EntityRef entity(String id) {
        return new EntityRef(id, id, Collections.emptyList(), "entity", Collections.emptyList());
    }
}