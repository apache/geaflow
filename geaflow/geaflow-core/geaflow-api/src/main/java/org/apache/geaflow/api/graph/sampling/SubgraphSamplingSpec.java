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

import java.io.Serializable;
import java.util.Objects;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

/**
 * Configuration for bounded iterative multi-hop sampling.
 */
public class SubgraphSamplingSpec implements Serializable {

    public static final long DEFAULT_MAX_SAMPLED_NODES = 10000L;
    public static final long DEFAULT_MAX_SAMPLED_EDGES = 100000L;
    public static final long DEFAULT_MAX_RETURNED_EDGES = 100000L;

    private final int hops;
    private final int fanout;
    private final EdgeDirection direction;
    private final long maxReturnedEdges;
    private final long seed;

    public SubgraphSamplingSpec(int hops, int fanout, EdgeDirection direction) {
        this(hops, fanout, direction, DEFAULT_MAX_RETURNED_EDGES);
    }

    public SubgraphSamplingSpec(int hops, int fanout, EdgeDirection direction,
                                long maxReturnedEdges) {
        this(hops, fanout, direction, maxReturnedEdges, 0L);
    }

    public SubgraphSamplingSpec(int hops, int fanout, EdgeDirection direction,
                                long maxReturnedEdges, long seed) {
        if (hops < 1) {
            throw new IllegalArgumentException("sampling hops must be greater than zero");
        }
        if (fanout == 0 || fanout < -1) {
            throw new IllegalArgumentException("fanout must be -1 or greater than zero");
        }
        if (maxReturnedEdges < 1) {
            throw new IllegalArgumentException("maxReturnedEdges must be greater than zero");
        }
        this.hops = hops;
        this.fanout = fanout;
        this.direction = Objects.requireNonNull(direction, "direction");
        this.maxReturnedEdges = maxReturnedEdges;
        this.seed = seed;
    }

    public int getHops() {
        return hops;
    }

    public int getFanout() {
        return fanout;
    }

    public EdgeDirection getDirection() {
        return direction;
    }

    public long getMaxReturnedEdges() {
        return maxReturnedEdges;
    }

    public long getSeed() {
        return seed;
    }
}
