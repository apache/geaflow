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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Deterministically truncates ranked evidence without splitting a source chunk. */
public final class TokenContextTruncator {
    private final TokenEstimator estimator;

    public TokenContextTruncator(TokenEstimator estimator) {
        if (estimator == null) {
            throw new IllegalArgumentException("estimator is required");
        }
        this.estimator = estimator;
    }

    public Result truncate(List<Evidence> ranked, int topK, int tokenBudget) {
        return truncate(ranked, topK, tokenBudget, true);
    }

    /** Truncates by topK while optionally treating the token budget as a hard limit. */
    public Result truncate(List<Evidence> ranked, int topK, long tokenBudget, boolean enforceTokenBudget) {
        if (topK < 1 || tokenBudget < 1) {
            throw new IllegalArgumentException("topK and tokenBudget must be positive");
        }
        List<Evidence> retained = new ArrayList<>();
        Set<String> chunks = new HashSet<>();
        long tokens = 0L;
        boolean skippedForBudget = false;
        for (Evidence evidence : ranked == null ? java.util.Collections.<Evidence>emptyList() : ranked) {
            if (retained.size() >= topK) {
                break;
            }
            long additional = 0L;
            Set<String> candidateChunks = new HashSet<>();
            for (TextChunk chunk : evidence.getChunks()) {
                if (chunks.add(chunk.getChunkId())) {
                    candidateChunks.add(chunk.getChunkId());
                    int estimate = estimator.estimate(chunk);
                    if (estimate < 1) {
                        throw new IllegalArgumentException("token estimate must be positive");
                    }
                    additional += estimate;
                }
            }
            if (enforceTokenBudget && (additional > tokenBudget || tokens > tokenBudget - additional)) {
                chunks.removeAll(candidateChunks);
                skippedForBudget = true;
                continue;
            }
            retained.add(evidence);
            tokens += additional;
        }
        RecallStopReason reason = skippedForBudget
            ? RecallStopReason.TOKEN_BUDGET : RecallStopReason.COMPLETED;
        return new Result(retained, tokens, reason);
    }

    public String getEstimatorVersion() {
        return estimator.getVersion();
    }

    /** Immutable truncation outcome with retained token accounting. */
    public static final class Result {
        private final List<Evidence> evidence;
        private final long tokensRetained;
        private final RecallStopReason stopReason;

        private Result(List<Evidence> evidence, long tokensRetained, RecallStopReason stopReason) {
            this.evidence = java.util.Collections.unmodifiableList(new ArrayList<>(evidence));
            this.tokensRetained = tokensRetained;
            this.stopReason = stopReason;
        }

        public List<Evidence> getEvidence() {
            return evidence;
        }

        public long getTokensRetained() {
            return tokensRetained;
        }

        public RecallStopReason getStopReason() {
            return stopReason;
        }
    }
}
