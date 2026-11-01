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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable request passed beyond the validation boundary. */
public final class RetrievalCommand {

    private final String graphName;
    private final String query;
    private final RetrievalMode mode;
    private final ExecutionMode executionMode;
    private final RetrievalBudget budget;
    private final List<Double> queryVector;
    private final int executionCandidateLimit;
    private final boolean exactRanking;

    public RetrievalCommand(String graphName, String query, RetrievalMode mode,
                            ExecutionMode executionMode, RetrievalBudget budget) {
        this(graphName, query, mode, executionMode, budget, Collections.<Double>emptyList());
    }

    public RetrievalCommand(String graphName, String query, RetrievalMode mode,
                            ExecutionMode executionMode, RetrievalBudget budget,
                            List<Double> queryVector) {
        this(graphName, query, mode, executionMode, budget, queryVector,
            budget.getMaxCandidates(), false);
    }

    public RetrievalCommand(String graphName, String query, RetrievalMode mode,
                            ExecutionMode executionMode, RetrievalBudget budget,
                            List<Double> queryVector, int executionCandidateLimit) {
        this(graphName, query, mode, executionMode, budget, queryVector, executionCandidateLimit, false);
    }

    public RetrievalCommand(String graphName, String query, RetrievalMode mode,
                            ExecutionMode executionMode, RetrievalBudget budget,
                            List<Double> queryVector, int executionCandidateLimit, boolean exactRanking) {
        if (executionCandidateLimit < 1) {
            throw new IllegalArgumentException("executionCandidateLimit must be positive");
        }
        this.graphName = graphName;
        this.query = query;
        this.mode = mode;
        this.executionMode = executionMode;
        this.executionCandidateLimit = executionCandidateLimit;
        this.exactRanking = exactRanking;
        this.budget = budget.copy();
        this.queryVector = queryVector == null ? Collections.<Double>emptyList()
            : Collections.unmodifiableList(new ArrayList<>(queryVector));
    }

    public String getGraphName() {
        return graphName;
    }

    public String getQuery() {
        return query;
    }

    public RetrievalMode getMode() {
        return mode;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public RetrievalBudget getBudget() {
        return budget.copy();
    }

    /** Returns the caller-supplied, precomputed query vector. */
    public List<Double> getQueryVector() {
        return queryVector;
    }

    /** Returns the service-wide candidate safety ceiling, independent of the primary constraint. */
    public int getExecutionCandidateLimit() {
        return executionCandidateLimit;
    }

    public boolean isExactRanking() {
        return exactRanking;
    }
}
