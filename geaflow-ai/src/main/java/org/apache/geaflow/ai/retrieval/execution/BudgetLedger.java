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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/** Thread-safe request budget ledger; each candidate and evidence token is charged at most once. */
public final class BudgetLedger {
    private final int candidateBudget;
    private final long tokenBudget;
    private final long deadlineNanos;
    private final LongSupplier clock;
    private final Set<String> retainedEvidence = new LinkedHashSet<>();
    private int candidatesEvaluated;
    private long tokensRetained;
    private RecallStopReason stopReason = RecallStopReason.COMPLETED;

    public BudgetLedger(int candidateBudget, long tokenBudget, long deadlineNanos, LongSupplier clock) {
        if (candidateBudget < 1 || tokenBudget < 1 || clock == null) {
            throw new IllegalArgumentException("ledger budgets and clock are required");
        }
        this.candidateBudget = candidateBudget;
        this.tokenBudget = tokenBudget;
        this.deadlineNanos = deadlineNanos;
        this.clock = clock;
    }

    public BudgetLedger(int candidateBudget, long tokenBudget, long deadlineNanos) {
        this(candidateBudget, tokenBudget, deadlineNanos, System::nanoTime);
    }

    /** Charges one scored candidate and returns false when the request cannot score another one. */
    public synchronized boolean tryEvaluateCandidate() {
        if (!checkDeadline() || candidatesEvaluated >= candidateBudget) {
            if (stopReason == RecallStopReason.COMPLETED) {
                stopReason = RecallStopReason.CANDIDATE_BUDGET;
            }
            return false;
        }
        candidatesEvaluated++;
        return true;
    }

    /** Charges unique evidence context; duplicate evidence does not consume token budget twice. */
    public synchronized boolean tryRetain(String evidenceId, TextChunk chunk, TokenEstimator estimator) {
        if (evidenceId == null || chunk == null || estimator == null) {
            throw new IllegalArgumentException("evidenceId, chunk, and estimator are required");
        }
        if (retainedEvidence.contains(evidenceId)) {
            return true;
        }
        if (!checkDeadline()) {
            return false;
        }
        int estimate = estimator.estimate(chunk);
        if (estimate < 1 || tokensRetained > tokenBudget - estimate) {
            stopReason = RecallStopReason.TOKEN_BUDGET;
            return false;
        }
        retainedEvidence.add(evidenceId);
        tokensRetained += estimate;
        return true;
    }

    public synchronized int getCandidatesEvaluated() {
        return candidatesEvaluated;
    }

    public synchronized long getTokensRetained() {
        return tokensRetained;
    }

    public int getCandidateBudget() {
        return candidateBudget;
    }

    public long getTokenBudget() {
        return tokenBudget;
    }

    public synchronized Set<String> getRetainedEvidence() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(retainedEvidence));
    }

    public synchronized RecallStopReason getStopReason() {
        return stopReason;
    }

    public synchronized boolean isExhausted() {
        return stopReason != RecallStopReason.COMPLETED;
    }

    private boolean checkDeadline() {
        if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
            stopReason = RecallStopReason.DEADLINE;
            return false;
        }
        return true;
    }
}
