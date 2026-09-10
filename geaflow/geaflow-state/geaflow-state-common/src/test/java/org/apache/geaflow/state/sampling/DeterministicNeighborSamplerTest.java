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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeterministicNeighborSamplerTest {

    @Test
    public void testSamplingIsBoundedAndIndependentOfInputOrder() {
        List<IEdge<Long, String>> first = Arrays.asList(
            edge(1L, 2L), edge(1L, 3L), edge(1L, 4L));
        List<IEdge<Long, String>> second = Arrays.asList(
            edge(1L, 4L), edge(1L, 2L), edge(1L, 3L));

        List<IEdge<Long, String>> sampledFirst =
            DeterministicNeighborSampler.sample(1L, first, EdgeDirection.OUT, 2,
                Long::compare, DeterministicNeighborSamplerTest::longBytes);
        List<IEdge<Long, String>> sampledSecond =
            DeterministicNeighborSampler.sample(1L, second, EdgeDirection.OUT, 2,
                Long::compare, DeterministicNeighborSamplerTest::longBytes);

        Assert.assertEquals(sampledFirst.size(), 2);
        Assert.assertEquals(targetIds(sampledFirst), targetIds(sampledSecond));
    }

    @Test
    public void testPositiveFanoutDoesNotMaterializeAllCandidates() {
        List<IEdge<Long, String>> edges = new ArrayList<>();
        for (long target = 2L; target < 102L; target++) {
            edges.add(edge(1L, target));
        }

        List<IEdge<Long, String>> sampled = DeterministicNeighborSampler.sample(
            1L, edges, EdgeDirection.OUT, 3, Long::compare, 3L, 17L, 9L,
            DeterministicNeighborSamplerTest::longBytes);

        Assert.assertEquals(sampled.stream().map(IEdge::getTargetId).distinct().count(), 3L);
        Assert.assertEquals(sampled.size(), 3);
    }

    @Test
    public void testSeedAndVersionAreStableAndInputOrderIndependent() {
        List<IEdge<Long, String>> first = new ArrayList<>();
        for (long target = 2L; target < 42L; target++) {
            first.add(edge(1L, target));
        }
        List<IEdge<Long, String>> reversed = new ArrayList<>(first);
        java.util.Collections.reverse(reversed);

        List<Long> firstSample = targetIds(DeterministicNeighborSampler.sample(
            1L, first, EdgeDirection.OUT, 5, Long::compare, 5L, 123L, 7L,
            DeterministicNeighborSamplerTest::longBytes));
        List<Long> reorderedSample = targetIds(DeterministicNeighborSampler.sample(
            1L, reversed, EdgeDirection.OUT, 5, Long::compare, 5L, 123L, 7L,
            DeterministicNeighborSamplerTest::longBytes));
        List<Long> nextVersionSample = targetIds(DeterministicNeighborSampler.sample(
            1L, first, EdgeDirection.OUT, 5, Long::compare, 5L, 123L, 8L,
            DeterministicNeighborSamplerTest::longBytes));

        Assert.assertEquals(firstSample, reorderedSample);
        Assert.assertNotEquals(firstSample, nextVersionSample);
    }

    @Test
    public void testDirectionAndUnlimitedFanout() {
        IEdge<Long, String> out = edge(1L, 2L);
        IEdge<Long, String> in = edge(1L, 3L);
        in.setDirect(EdgeDirection.IN);

        List<IEdge<Long, String>> sampledIn = DeterministicNeighborSampler.sample(
            1L, Arrays.asList(out, in), EdgeDirection.IN, -1, Long::compare,
            DeterministicNeighborSamplerTest::longBytes);
        Assert.assertEquals(sampledIn.size(), 1);
        Assert.assertEquals(sampledIn.get(0).getSrcId(), Long.valueOf(3L));
        Assert.assertEquals(sampledIn.get(0).getTargetId(), Long.valueOf(1L));
        Assert.assertEquals(sampledIn.get(0).getDirect(), EdgeDirection.OUT);
        Assert.assertEquals(
            DeterministicNeighborSampler.sample(1L, Arrays.asList(out, in), EdgeDirection.BOTH,
                -1, Long::compare, DeterministicNeighborSamplerTest::longBytes).size(),
            2);
    }

    @Test
    public void testIgnoresUnrelatedEdges() {
        IEdge<Long, String> related = edge(1L, 2L);
        IEdge<Long, String> unrelated = edge(3L, 4L);
        List<IEdge<Long, String>> edges = Arrays.asList(related, unrelated);

        List<IEdge<Long, String>> sampled = DeterministicNeighborSampler.sample(
            1L, edges, EdgeDirection.OUT, -1, Long::compare, 1L, 0L, 0L,
            DeterministicNeighborSamplerTest::longBytes);
        List<IEdge<Long, String>> projected = DeterministicNeighborSampler.project(
            1L, edges, EdgeDirection.OUT, -1, Long::compare,
            DeterministicNeighborSamplerTest::longBytes);

        Assert.assertEquals(sampled, Collections.singletonList(related));
        Assert.assertEquals(projected, Collections.singletonList(related));
    }

    @Test
    public void testIncomingNormalizationDoesNotMutateInputEdge() {
        IEdge<Long, String> incoming = edge(2L, 1L);
        incoming.setDirect(EdgeDirection.IN);

        List<IEdge<Long, String>> sampled = DeterministicNeighborSampler.sample(
            1L, Collections.singletonList(incoming), EdgeDirection.IN, -1, Long::compare,
            DeterministicNeighborSamplerTest::longBytes);

        Assert.assertEquals(incoming.getSrcId(), Long.valueOf(2L));
        Assert.assertEquals(incoming.getTargetId(), Long.valueOf(1L));
        Assert.assertEquals(incoming.getDirect(), EdgeDirection.IN);
        Assert.assertEquals(sampled.get(0).getSrcId(), Long.valueOf(1L));
        Assert.assertEquals(sampled.get(0).getTargetId(), Long.valueOf(2L));
        Assert.assertEquals(sampled.get(0).getDirect(), EdgeDirection.OUT);
    }

    @Test
    public void testComparatorTieUsesStableIdFallback() {
        List<IEdge<Long, String>> edges = Arrays.asList(edge(1L, 3L), edge(1L, 2L));

        List<Long> first = targetIds(DeterministicNeighborSampler.sample(
            1L, edges, EdgeDirection.OUT, 1, (left, right) -> 0,
            100L, 17L, 7L, DeterministicNeighborSamplerTest::longBytes));
        List<Long> second = targetIds(DeterministicNeighborSampler.sample(
            1L, Arrays.asList(edges.get(1), edges.get(0)), EdgeDirection.OUT, 1,
            (left, right) -> 0, 100L, 17L, 7L,
            DeterministicNeighborSamplerTest::longBytes));

        Assert.assertEquals(first, second);
        Assert.assertEquals(first.size(), 1);
    }

    @Test
    public void testFanoutCountsNeighborsAndKeepsSelectedParallelEdges() {
        List<IEdge<Long, String>> edges = Arrays.asList(
            edge(1L, 2L), edgeWithValue(1L, 2L, "parallel"), edge(1L, 3L), edge(1L, 4L));

        List<IEdge<Long, String>> sampled = DeterministicNeighborSampler.sample(
            1L, edges, EdgeDirection.OUT, 2, Long::compare,
            DeterministicNeighborSamplerTest::longBytes);
        Map<Long, Long> allCounts = edges.stream().collect(Collectors.groupingBy(
            IEdge::getTargetId, Collectors.counting()));
        Map<Long, Long> sampledCounts = sampled.stream().collect(Collectors.groupingBy(
            IEdge::getTargetId, Collectors.counting()));

        Assert.assertEquals(sampledCounts.size(), 2);
        for (Map.Entry<Long, Long> entry : sampledCounts.entrySet()) {
            Assert.assertEquals(entry.getValue(), allCounts.get(entry.getKey()));
        }
    }

    @Test
    public void testNeighborhoodMatchesSnapshotAndSamplingVersion() {
        LocalNeighborhood<Long, String, String> neighborhood = new LocalNeighborhood<>(
            new ValueVertex<>(1L, "vertex"), Arrays.asList(edge(1L, 2L)), 7L, 3L);

        Assert.assertTrue(neighborhood.matches(7L, 3L));
        Assert.assertFalse(neighborhood.matches(8L, 3L));
        Assert.assertFalse(neighborhood.matches(7L, 4L));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRejectsReturnedEdgeOverflow() {
        DeterministicNeighborSampler.sample(1L,
            Arrays.asList(edge(1L, 2L), edge(1L, 3L)), EdgeDirection.OUT, -1,
            Long::compare, 1, 0L, 0L, DeterministicNeighborSamplerTest::longBytes);
    }

    @Test
    public void testSamplesIdsWithoutCallingToString() {
        List<IEdge<StableId, String>> edges = Arrays.asList(
            stableEdge(1L, 2L, "first"), stableEdge(1L, 2L, "second"),
            stableEdge(1L, 3L, "third"));
        Comparator<StableId> comparator = (left, right) -> 0;

        List<IEdge<StableId, String>> first = DeterministicNeighborSampler.sample(
            new StableId(1L), edges, EdgeDirection.OUT, -1, comparator, 100L, 17L, 7L,
            id -> longBytes(id.value));
        List<IEdge<StableId, String>> second = DeterministicNeighborSampler.sample(
            new StableId(1L), Arrays.asList(edges.get(2), edges.get(1), edges.get(0)),
            EdgeDirection.OUT, -1, comparator, 100L, 17L, 7L, id -> longBytes(id.value));

        if (first.size() != second.size()) {
            Assert.fail("sampling result sizes differ");
        }
        for (int i = 0; i < first.size(); i++) {
            if (first.get(i) != second.get(i)) {
                Assert.fail("sampling result order differs");
            }
        }
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "idEncoder result")
    public void testRejectsNullIdEncoding() {
        DeterministicNeighborSampler.sample(1L, Collections.singletonList(edge(1L, 2L)),
            EdgeDirection.OUT, -1, Long::compare, 100L, 0L, 0L, id -> null);
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "idEncoder")
    public void testRejectsNullIdEncoder() {
        DeterministicNeighborSampler.sample(1L, Collections.singletonList(edge(1L, 2L)),
            EdgeDirection.OUT, -1, Long::compare, 100L, 0L, 0L,
            (Function<Long, byte[]>) null);
    }

    private static IEdge<Long, String> edge(long source, long target) {
        return edgeWithValue(source, target, "value");
    }

    private static IEdge<Long, String> edgeWithValue(long source, long target, String value) {
        ValueEdge<Long, String> edge = new ValueEdge<>(source, target, value);
        edge.setDirect(EdgeDirection.OUT);
        return edge;
    }

    private static List<Long> targetIds(List<IEdge<Long, String>> edges) {
        return edges.stream().map(IEdge::getTargetId).collect(Collectors.toList());
    }

    private static IEdge<StableId, String> stableEdge(long source, long target, String value) {
        ValueEdge<StableId, String> edge = new ValueEdge<>(new StableId(source),
            new StableId(target), value);
        edge.setDirect(EdgeDirection.OUT);
        return edge;
    }

    private static byte[] longBytes(long value) {
        return new byte[]{
            (byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40),
            (byte) (value >>> 32), (byte) (value >>> 24), (byte) (value >>> 16),
            (byte) (value >>> 8), (byte) value};
    }

    private static final class StableId {

        private final long value;

        private StableId(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof StableId && value == ((StableId) other).value;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }

        @Override
        public String toString() {
            throw new AssertionError("sampling must not call StableId.toString()");
        }
    }

}
