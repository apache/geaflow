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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api.model;

/** Retrieval modes understood by the versioned API. */
public enum RetrievalMode {
    /** Legacy keyword mode, equivalent to BM25_ONLY. */
    KEYWORD,
    BM25_ONLY,
    VECTOR_ONLY,
    GRAPH_ONLY,
    /** Hybrid retrieval with fixed channel weights and no routing policy. */
    FIXED_HYBRID,
    /** Hybrid retrieval whose plan comes from the adaptive routing policy. */
    ADAPTIVE_HYBRID,
    /**
     * Compatibility alias for a hybrid request that does not state which policy applies.
     *
     * <p>It executes the hybrid channel set; the routing policy is consulted only for
     * {@link #ADAPTIVE_HYBRID}, so a caller that needs adaptive behaviour should ask for it
     * explicitly instead of relying on an execution-mode side effect.
     */
    HYBRID;

    /** Returns the channel-set mode, collapsing the aliases onto their concrete counterpart. */
    public RetrievalMode canonical() {
        if (this == KEYWORD) {
            return BM25_ONLY;
        }
        if (this == FIXED_HYBRID || this == ADAPTIVE_HYBRID) {
            return HYBRID;
        }
        return this;
    }

    /** Returns whether this mode selects the adaptive routing policy. */
    public boolean isAdaptive() {
        return this == ADAPTIVE_HYBRID;
    }

    /** Returns whether this mode executes the hybrid channel set. */
    public boolean isHybrid() {
        return canonical() == HYBRID;
    }
}
