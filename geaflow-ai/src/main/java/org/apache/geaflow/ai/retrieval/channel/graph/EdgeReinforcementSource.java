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

import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/**
 * Optional capability for a {@link GraphNeighborProvider} that can report how often an edge was
 * reinforced by incremental ingestion and how old it is.
 *
 * <p>The design allows edge strength to combine repeated incremental reinforcement and an optional
 * time-decay factor. Those inputs belong to the storage layer, so this is a narrow optional
 * interface: providers without reinforcement history simply do not implement it, and the edge
 * strength model then sees a neutral count and age.
 *
 * <p>{@link GraphNeighborProvider} does not change, which keeps existing providers and the
 * {@code GraphAccessor.scanEdge(...)} adapter valid.
 */
public interface EdgeReinforcementSource {

    /**
     * Returns how many times this edge was reinforced by incremental ingestion.
     *
     * @param edge edge being scored
     * @return non-negative reinforcement count, {@code 0} when never reinforced
     */
    int getReinforcementCount(GraphEdgeRef edge);

    /**
     * Returns the age of this edge in milliseconds relative to the graph version under observation.
     *
     * @param edge edge being scored
     * @return non-negative age in milliseconds, {@code 0} when unknown or freshly written
     */
    long getAgeMillis(GraphEdgeRef edge);
}