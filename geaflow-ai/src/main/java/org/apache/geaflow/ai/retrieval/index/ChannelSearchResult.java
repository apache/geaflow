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
package org.apache.geaflow.ai.retrieval.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;

/** Search hits and execution counters produced by one recall channel. */
public final class ChannelSearchResult<T> {
    private final List<T> hits;
    private final int candidatesEvaluated;
    private final RecallStopReason stopReason;

    public ChannelSearchResult(List<T> hits, int candidatesEvaluated, boolean deadlineReached) {
        this(hits, candidatesEvaluated, deadlineReached ? RecallStopReason.DEADLINE : RecallStopReason.COMPLETED);
    }

    public ChannelSearchResult(List<T> hits, int candidatesEvaluated, RecallStopReason stopReason) {
        this.hits = Collections.unmodifiableList(new ArrayList<>(hits));
        this.candidatesEvaluated = candidatesEvaluated;
        this.stopReason = stopReason;
    }

    public List<T> getHits() {
        return hits;
    }

    public int getCandidatesEvaluated() {
        return candidatesEvaluated;
    }

    public RecallStopReason getStopReason() {
        return stopReason;
    }

    public boolean isDeadlineReached() {
        return stopReason == RecallStopReason.DEADLINE;
    }
}
