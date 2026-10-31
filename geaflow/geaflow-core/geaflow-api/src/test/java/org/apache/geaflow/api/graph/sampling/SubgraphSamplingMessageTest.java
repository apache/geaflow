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

import java.util.Collections;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.sampling.LocalNeighborhood;
import org.testng.annotations.Test;

public class SubgraphSamplingMessageTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNonPositiveRequestDepth() {
        new SubgraphSamplingRequest<>(1L, 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRejectsNullRequestRoot() {
        new SubgraphSamplingRequest<>(null, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNonPositiveResponseDepth() {
        new SubgraphSamplingResponse<>(1L, 0, neighborhood(1L));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRejectsNullResponseRoot() {
        new SubgraphSamplingResponse<>(null, 1, neighborhood(1L));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRejectsNullResponseNeighborhood() {
        new SubgraphSamplingResponse<>(1L, 1, null);
    }

    private LocalNeighborhood<Long, Integer, Integer> neighborhood(long vertexId) {
        return new LocalNeighborhood<>(new ValueVertex<>(vertexId, (int) vertexId),
            Collections.emptyList(), 7L);
    }
}
