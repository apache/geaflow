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

package org.apache.geaflow.ai.retrieval.channel.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.codec.GraphDeltaJson;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

class VersionedGraphViewTest {

    @Test
    void mergesDeltaAndTombstonesLikeFullRebuild() {
        GraphVersion snapshotVersion = new GraphVersion("knowledge", "s1");
        EntityRef alpha = entity("alpha", "Alpha");
        EntityRef beta = entity("beta", "Beta");
        EntityRef gamma = entity("gamma", "Gamma");
        GraphEdgeRef ab = edge("ab", "alpha", "beta", "related");
        GraphEdgeRef bc = edge("bc", "beta", "gamma", "related");
        GraphSnapshot snapshot = new GraphSnapshot(snapshotVersion,
            Arrays.asList(alpha, beta, gamma), Arrays.asList(ab, bc));

        GraphVersion deltaVersion = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(deltaVersion, snapshotVersion)
            .upsertEntity(entity("delta", "Delta"))
            .upsertEdge(edge("bd", "beta", "delta", "mentions"))
            .deleteEdge("bc")
            .deleteEntity("alpha")
            .build();

        VersionedGraphView view = VersionedGraphView.from(snapshot, delta);
        assertEquals(Collections.singletonList(edge("bd", "beta", "delta", "mentions")),
            view.neighbors("beta"));
        assertTrue(view.neighbors("alpha").isEmpty());
        assertEquals(1, view.statistics("beta").getDegree());
        assertEquals(1, view.statistics("beta").getRelationCounts().get("mentions").intValue());
        assertEquals(deltaVersion, view.getGraphVersion());
        assertEquals(view.getFingerprint(), VersionedGraphView.from(snapshot, delta).getFingerprint());
    }

    @Test
    void rejectsMissingParentConflictingDeltaAndBadReferences() {
        GraphVersion snapshotVersion = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(snapshotVersion,
            Collections.singletonList(entity("alpha", "Alpha")), Collections.emptyList());
        GraphVersion deltaVersion = new GraphVersion("knowledge", "s2");
        assertThrows(GraphViewException.class, () -> GraphDelta.builder(deltaVersion,
            new GraphVersion("knowledge", "missing")).upsertEntity(entity("beta", "Beta")).build()
            .applyTo(snapshot));
        assertThrows(GraphViewException.class, () -> GraphDelta.builder(deltaVersion, snapshotVersion)
            .upsertEntity(entity("beta", "Beta"))
            .deleteEntity("beta")
            .build());
        assertThrows(GraphViewException.class, () -> GraphDelta.builder(deltaVersion, snapshotVersion)
            .upsertEdge(edge("bad", "alpha", "missing", "related"))
            .build()
            .applyTo(snapshot));
    }

    @Test
    void leaseKeepsOldViewAfterRegistryPublication() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(first,
            Arrays.asList(entity("a", "A"), entity("b", "B")),
            Collections.singletonList(edge("ab", "a", "b", "related")));
        VersionedGraphViewRegistry registry = new VersionedGraphViewRegistry();
        registry.register(snapshot);
        registry.publish(first, null);
        GraphQueryLease lease = registry.openLease(first);

        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(second, first).deleteEdge("ab").build();
        registry.register(delta);
        registry.publish(second, first);

        assertEquals(1, lease.neighbors("a").size());
        assertTrue(registry.openLease(second).neighbors("a").isEmpty());
        lease.close();
    }

    @Test
    void deltaDigestIsStableAcrossConstructionPathsAndEmptyForSnapshot() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(first,
            Arrays.asList(entity("a", "A"), entity("b", "B"), entity("c", "C")),
            Arrays.asList(edge("ab", "a", "b", "related"), edge("bc", "b", "c", "related")));
        assertEquals("", VersionedGraphView.from(snapshot).getDeltaDigest());

        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphVersion third = new GraphVersion("knowledge", "s3");
        GraphDelta firstDelta = GraphDelta.builder(second, first).deleteEdge("bc").build();
        GraphDelta secondDelta = GraphDelta.builder(third, second).deleteEdge("ab").build();
        VersionedGraphView flat = VersionedGraphView.from(snapshot, firstDelta, secondDelta);
        VersionedGraphView chained = VersionedGraphView.from(
            VersionedGraphView.from(snapshot, firstDelta), secondDelta);
        assertEquals(flat.getDeltaDigest(), chained.getDeltaDigest());
        assertEquals(flat.getFingerprint(), chained.getFingerprint());
    }

    @Test
    void appliesDeltaChainInOneSnapshotAndMatchesSequentialRebuild() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(first,
            Arrays.asList(entity("a", "A"), entity("b", "B")),
            Collections.singletonList(edge("ab", "a", "b", "related")));
        String originalFingerprint = snapshot.getFingerprint();

        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphDelta firstDelta = GraphDelta.builder(second, first)
            .upsertEntity(entityWithAlias("a", "A", "alias"))
            .upsertEntity(entity("c", "C"))
            .upsertEdge(edge("ac", "a", "c", "mentions"))
            .deleteEdge("ab")
            .build();
        GraphVersion third = new GraphVersion("knowledge", "s3");
        GraphDelta secondDelta = GraphDelta.builder(third, second)
            .upsertEntity(entity("d", "D"))
            .upsertEdge(edge("ad", "a", "d", "mentions"))
            .deleteAlias("a", "alias")
            .deleteEntity("c")
            .build();

        GraphSnapshot sequential = secondDelta.applyTo(firstDelta.applyTo(snapshot));
        VersionedGraphView batched = VersionedGraphView.from(snapshot,
            Arrays.asList(firstDelta, secondDelta));

        assertEquals(sequential.getFingerprint(), batched.getSnapshot().getFingerprint());
        assertEquals(Collections.singletonList(edge("ad", "a", "d", "mentions")),
            batched.neighbors("a"));
        assertTrue(batched.neighbors("c").isEmpty());
        assertEquals(Collections.emptyList(), batched.getSnapshot().getEntities().get(0).getAliases());
        assertEquals(originalFingerprint, snapshot.getFingerprint());
    }

    @Test
    void graphDeltaJsonRoundTripsOperationsAndVerifiesDigest() throws Exception {
        GraphVersion parent = new GraphVersion("knowledge", "s1");
        GraphVersion version = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(version, parent)
            .upsertEntity(entityWithAlias("b", "Beta", "B"))
            .upsertEdge(edge("ab", "a", "b", "related"))
            .deleteEntity("a")
            .deleteVertex("vertex-a")
            .deleteAlias("b", "old-name")
            .build();
        StringWriter output = new StringWriter();
        GraphDeltaJson.write(delta, output);

        GraphDelta decoded = GraphDeltaJson.read(new BufferedReader(new StringReader(output.toString())),
            version, parent, delta.getDigest());
        assertEquals(delta.getDigest(), decoded.getDigest());
        assertEquals(delta.getEntities(), decoded.getEntities());
        assertEquals(delta.getEdges(), decoded.getEdges());
        assertEquals(delta.getTombstones().getEntities(), decoded.getTombstones().getEntities());
        assertEquals(delta.getTombstones().getVertices(), decoded.getTombstones().getVertices());
        assertThrows(java.io.IOException.class, () -> GraphDeltaJson.read(
            new BufferedReader(new StringReader(output.toString())), version, parent, "invalid"));
    }

    @Test
    void rejectsNullInputsAndUnknownTombstonesWithTypedErrors() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(first,
            Collections.singletonList(entity("a", "A")), Collections.emptyList());
        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphViewException nullEntity = assertThrows(GraphViewException.class,
            () -> GraphDelta.builder(second, first).upsertEntity(null));
        assertEquals(GraphViewException.Code.INVALID_VIEW, nullEntity.getCode());
        GraphViewException missingEdge = assertThrows(GraphViewException.class,
            () -> GraphDelta.builder(second, first).deleteEdge("missing").build().applyTo(snapshot));
        assertEquals(GraphViewException.Code.BAD_REFERENCE, missingEdge.getCode());
        GraphViewException aliasConflict = assertThrows(GraphViewException.class,
            () -> GraphDelta.builder(second, first)
                .upsertEntity(entityWithAlias("a", "A", "alias"))
                .deleteAlias("a", "alias").build());
        assertEquals(GraphViewException.Code.DELTA_CONFLICT, aliasConflict.getCode());
        GraphViewException nullVersion = assertThrows(GraphViewException.class,
            () -> new VersionedGraphViewRegistry().openLease(null));
        assertEquals(GraphViewException.Code.INVALID_VIEW, nullVersion.getCode());
    }

    @Test
    void allowsStagedViewToBecomeReadyBeforePublishing() {
        GraphVersion version = new GraphVersion("knowledge", "staged");
        GraphSnapshot snapshot = new GraphSnapshot(version,
            Collections.singletonList(entity("a", "A")), Collections.emptyList());
        VersionedGraphViewRegistry registry = new VersionedGraphViewRegistry();
        registry.register(snapshot, false);
        assertThrows(GraphViewException.class, () -> registry.openLease(version));
        registry.markReady(version);
        registry.publish(version, null);
        assertEquals(version, registry.openPublishedLease().getGraphVersion());
    }

    @Test
    void registryDoesNotPublishChildOfUnreadyParent() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        GraphSnapshot snapshot = new GraphSnapshot(first,
            Collections.singletonList(entity("a", "A")), Collections.emptyList());
        VersionedGraphViewRegistry registry = new VersionedGraphViewRegistry();
        registry.register(snapshot, false);
        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(second, first).build();
        GraphViewException error = assertThrows(GraphViewException.class, () -> registry.register(delta));
        assertEquals(GraphViewException.Code.NOT_READY, error.getCode());
    }

    @Test
    void vertexIdReplacementMasksPreviousEntityInOverlay() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        EntityRef alpha = entity("alpha", "Alpha");
        EntityRef beta = entity("beta", "Beta");
        GraphSnapshot snapshot = new GraphSnapshot(first, Collections.emptyList(),
            Arrays.asList(alpha, beta), Arrays.asList(
                new GraphVertexRef("person", "shared-vertex", "alpha")), Collections.emptyList());
        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(second, first)
            .upsertVertex(new GraphVertexRef("topic", "shared-vertex", "beta"))
            .build();

        VersionedGraphView view = VersionedGraphView.from(VersionedGraphView.from(snapshot), delta);

        assertEquals("alpha", view.vertex(alpha).getVertexId());
        assertEquals("shared-vertex", view.vertex(beta).getVertexId());
        assertEquals(Collections.singletonList("beta"), view.getSnapshot().getVertices().stream()
            .map(GraphVertexRef::getEntityId).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void vertexTombstoneRemovesMappingWithoutDeletingEntity() {
        GraphVersion first = new GraphVersion("knowledge", "s1");
        EntityRef alpha = entity("alpha", "Alpha");
        GraphSnapshot snapshot = new GraphSnapshot(first, Collections.emptyList(),
            Collections.singletonList(alpha),
            Collections.singletonList(new GraphVertexRef("person", "vertex-alpha", "alpha")),
            Collections.emptyList());
        GraphVersion second = new GraphVersion("knowledge", "s2");
        GraphDelta delta = GraphDelta.builder(second, first).deleteVertex("vertex-alpha").build();

        VersionedGraphView view = VersionedGraphView.from(VersionedGraphView.from(snapshot), delta);

        assertEquals("alpha", view.vertex(alpha).getVertexId());
        assertTrue(view.getSnapshot().getVertices().isEmpty());
        assertEquals(Collections.singletonList(alpha), view.getSnapshot().getEntities());
    }

    private static EntityRef entity(String id, String name) {
        return new EntityRef(id, name, Collections.emptyList(), "entity", Collections.emptyList());
    }

    private static EntityRef entityWithAlias(String id, String name, String alias) {
        return new EntityRef(id, name, Collections.singletonList(alias), "entity", Collections.emptyList());
    }

    private static GraphEdgeRef edge(String id, String source, String target, String label) {
        return new GraphEdgeRef(id, label, source, target, Collections.emptyList());
    }
}
