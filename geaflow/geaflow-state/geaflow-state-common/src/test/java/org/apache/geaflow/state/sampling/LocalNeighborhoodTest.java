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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalNeighborhoodTest {

    @Test
    public void testProjectPreservesVersionsAndBoundsNeighbors() {
        LocalNeighborhood<Long, Integer, Integer> neighborhood = neighborhood(
            Arrays.asList(edge(1L, 2L), edge(1L, 3L)), 7L, 11L);

        LocalNeighborhood<Long, Integer, Integer> projected = neighborhood.project(
            EdgeDirection.OUT, 1, Long::compare, 100L, 17L, LocalNeighborhoodTest::longBytes);

        Assert.assertEquals(projected.getEdges().size(), 1);
        Assert.assertEquals(projected.getSnapshotVersion(), 7L);
        Assert.assertEquals(projected.getSamplingVersion(), 11L);
        Assert.assertTrue(projected.matches(7L, 11L));
    }

    @Test
    public void testRevalidatePreservesSamplingVersionOnNewSnapshot() {
        LocalNeighborhood<Long, Integer, Integer> neighborhood = neighborhood(
            Collections.singletonList(edge(1L, 2L)), 7L, 11L);

        LocalNeighborhood<Long, Integer, Integer> revalidated = neighborhood.revalidate(
            new ValueVertex<>(1L, 2), 8L);

        Assert.assertEquals(revalidated.getSnapshotVersion(), 8L);
        Assert.assertEquals(revalidated.getSamplingVersion(), 11L);
        Assert.assertEquals(revalidated.getEdges().size(), 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsRevalidationToOlderSnapshot() {
        LocalNeighborhood<Long, Integer, Integer> neighborhood = neighborhood(
            Collections.emptyList(), 7L, 11L);

        neighborhood.revalidate(new ValueVertex<>(1L, 2), 6L);
    }

    @Test
    public void testCopiesInputEdgesAndProtectsReturnedEdges() {
        List<IEdge<Long, Integer>> input = new ArrayList<>();
        input.add(edge(1L, 2L));
        LocalNeighborhood<Long, Integer, Integer> neighborhood = neighborhood(input, 7L, 11L);

        input.clear();
        Assert.assertEquals(neighborhood.getEdges().size(), 1);
        try {
            neighborhood.getEdges().clear();
            Assert.fail("neighborhood edges must be immutable");
        } catch (UnsupportedOperationException expected) {
            // Expected defensive view.
        }
    }

    private LocalNeighborhood<Long, Integer, Integer> neighborhood(
        List<IEdge<Long, Integer>> edges, long snapshotVersion, long samplingVersion) {
        return new LocalNeighborhood<>(new ValueVertex<>(1L, 1), edges,
            snapshotVersion, samplingVersion);
    }

    private IEdge<Long, Integer> edge(long source, long target) {
        return new ValueEdge<>(source, target, 1, EdgeDirection.OUT);
    }

    private static byte[] longBytes(long value) {
        return new byte[]{
            (byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40),
            (byte) (value >>> 32), (byte) (value >>> 24), (byte) (value >>> 16),
            (byte) (value >>> 8), (byte) value};
    }
}
