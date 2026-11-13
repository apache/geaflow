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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Immutable sequential plan with fixed channel allocations and one absolute deadline. */
public final class RecallPlan {
    private final RetrievalMode mode;
    private final int topK;
    private final long deadlineNanos;
    private final Map<String, Integer> channelBudgets;
    private final String graphVersion;
    private final String indexVersion;
    private final String vectorVersion;

    public RecallPlan(RetrievalMode mode, int topK, int candidates, long deadlineNanos) {
        this(mode, topK, candidates, deadlineNanos, null, null, null);
    }

    public RecallPlan(RetrievalMode mode, int topK, int candidates, long deadlineNanos,
                      String graphVersion, String indexVersion, String vectorVersion) {
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.vectorVersion = vectorVersion;
        if (mode == null || topK < 1 || topK > candidates
            || (mode.canonical() == RetrievalMode.HYBRID && candidates < 3)) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid recall budget or mode");
        }
        this.mode = mode.canonical();
        this.topK = topK;
        this.deadlineNanos = deadlineNanos;
        Map<String, Integer> budgets = new LinkedHashMap<>();
        if (this.mode == RetrievalMode.HYBRID) {
            int base = candidates / 3;
            int remainder = candidates % 3;
            budgets.put("BM25", base + (remainder >= 1 ? 1 : 0));
            budgets.put("VECTOR", base + (remainder >= 2 ? 1 : 0));
            budgets.put("GRAPH", base);
        } else {
            budgets.put(this.mode == RetrievalMode.BM25_ONLY ? "BM25"
                : this.mode == RetrievalMode.VECTOR_ONLY ? "VECTOR" : "GRAPH", candidates);
        }
        channelBudgets = Collections.unmodifiableMap(budgets);
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public RetrievalMode getMode() {
        return mode;
    }

    public int getTopK() {
        return topK;
    }

    public long getDeadlineNanos() {
        return deadlineNanos;
    }

    public Map<String, Integer> getChannelBudgets() {
        return channelBudgets;
    }
}
