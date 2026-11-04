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
package org.apache.geaflow.ai.retrieval.metadata;

import java.util.Objects;

/**
 * The retrieval budget a dataset build was produced and intended to be evaluated under.
 *
 * <p>The design lists "budget configurations" among the eleven values every data build must store,
 * so that a published dataset is reproducible together with the budget it was benchmarked at. Before
 * this type existed the budget lived only in ad-hoc runner arguments and could not be recovered from
 * the build artifacts.
 */
public final class BudgetConfiguration {
    private final String primaryConstraint;
    private final int maxCandidates;
    private final int timeoutMs;
    private final int tokenBudget;
    private final int maxEdgeScans;

    /**
     * Creates a recorded budget configuration.
     *
     * @param primaryConstraint the ONE hard constraint the run spends: candidates, latency or tokens
     * @param maxCandidates candidate evaluation budget across channels
     * @param timeoutMs wall-clock deadline in milliseconds
     * @param tokenBudget retained context-token budget
     * @param maxEdgeScans graph edge-scan work cap, independent of the candidate budget
     */
    public BudgetConfiguration(String primaryConstraint, int maxCandidates, int timeoutMs,
                               int tokenBudget, int maxEdgeScans) {
        this.primaryConstraint = constraint(primaryConstraint);
        this.maxCandidates = positive(maxCandidates, "maxCandidates");
        this.timeoutMs = positive(timeoutMs, "timeoutMs");
        this.tokenBudget = positive(tokenBudget, "tokenBudget");
        this.maxEdgeScans = positive(maxEdgeScans, "maxEdgeScans");
    }

    /** Returns the budget this repository's pipeline uses by default. */
    public static BudgetConfiguration standard() {
        return new BudgetConfiguration("candidates", 100, 10000, 4096, 100);
    }

    public String getPrimaryConstraint() {
        return primaryConstraint;
    }

    public int getMaxCandidates() {
        return maxCandidates;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public int getTokenBudget() {
        return tokenBudget;
    }

    public int getMaxEdgeScans() {
        return maxEdgeScans;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof BudgetConfiguration)) {
            return false;
        }
        BudgetConfiguration that = (BudgetConfiguration) object;
        return maxCandidates == that.maxCandidates && timeoutMs == that.timeoutMs
            && tokenBudget == that.tokenBudget && maxEdgeScans == that.maxEdgeScans
            && Objects.equals(primaryConstraint, that.primaryConstraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryConstraint, maxCandidates, timeoutMs, tokenBudget, maxEdgeScans);
    }

    @Override
    public String toString() {
        return "BudgetConfiguration{" + primaryConstraint + ", candidates=" + maxCandidates
            + ", timeoutMs=" + timeoutMs + ", tokens=" + tokenBudget
            + ", edgeScans=" + maxEdgeScans + '}';
    }

    private static String constraint(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw MetadataValidation.invalid("primaryConstraint is required");
        }
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        if (!"candidates".equals(normalized) && !"latency".equals(normalized)
            && !"tokens".equals(normalized)) {
            throw MetadataValidation.invalid(
                "primaryConstraint must be candidates, latency or tokens");
        }
        return normalized;
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw MetadataValidation.invalid(name + " must be positive");
        }
        return value;
    }
}