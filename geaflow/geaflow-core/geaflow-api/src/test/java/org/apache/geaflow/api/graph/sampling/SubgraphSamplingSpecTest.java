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

package org.apache.geaflow.api.graph.sampling;

import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubgraphSamplingSpecTest {

    @Test
    public void testHopsAndFanoutAreScalar() {
        SubgraphSamplingSpec spec = new SubgraphSamplingSpec(2, 10, EdgeDirection.OUT);

        Assert.assertEquals(spec.getHops(), 2);
        Assert.assertEquals(spec.getFanout(), 10);
        Assert.assertEquals(spec.getMaxReturnedEdges(), 100000L);
        Assert.assertEquals(spec.getSeed(), 0L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNonPositiveHops() {
        new SubgraphSamplingSpec(0, 10, EdgeDirection.OUT);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsZeroFanout() {
        new SubgraphSamplingSpec(2, 0, EdgeDirection.OUT);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsFanoutBelowUnlimitedMarker() {
        new SubgraphSamplingSpec(2, -2, EdgeDirection.OUT);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRejectsNullDirection() {
        new SubgraphSamplingSpec(2, 1, null);
    }

    @Test
    public void testUnlimitedFanoutKeepsPerVertexEdgeBudget() {
        SubgraphSamplingSpec spec = new SubgraphSamplingSpec(2, -1, EdgeDirection.BOTH);

        Assert.assertEquals(spec.getFanout(), -1);
        Assert.assertEquals(spec.getMaxReturnedEdges(), 100000L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsInvalidReturnedEdgeBudget() {
        new SubgraphSamplingSpec(2, -1, EdgeDirection.BOTH, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNegativeReturnedEdgeBudget() {
        new SubgraphSamplingSpec(2, -1, EdgeDirection.BOTH, -1);
    }
}
