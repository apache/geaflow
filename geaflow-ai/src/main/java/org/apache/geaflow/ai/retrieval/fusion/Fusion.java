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
package org.apache.geaflow.ai.retrieval.fusion;

import java.util.List;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/**
 * Pluggable strategy that turns per-channel scored evidence into one deterministic ordering.
 *
 * <p>Weighted reciprocal rank fusion is the mandatory default implementation. Replacing it must
 * not change the execution mode, the budget accounting or the trace shape, so implementations are
 * expected to be pure functions of the evidence they receive.
 */
public interface Fusion {

    /** Configures channel weights and rank behaviour; also used to snapshot the trace. */
    WeightedRrfConfig getConfig();

    /** Returns the contribution of one evidence item before channel rank aggregation. */
    double score(Evidence evidence);

    /** Orders evidence deterministically; equal scores must fall back to a stable tie-break. */
    List<Evidence> sort(List<Evidence> evidence);

    /**
     * Pins the best item of one channel into the head of the ranking so a channel that produced
     * strong evidence is not erased by rank aggregation.
     */
    List<Evidence> preserveTopKChannel(List<Evidence> ranked, int topK, String channel,
                                       int minimumCount);

    /** Stable identifier recorded in the trace so a ranking can be replayed. */
    String getVersion();
}