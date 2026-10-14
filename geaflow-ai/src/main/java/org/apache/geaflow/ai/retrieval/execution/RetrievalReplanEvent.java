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
import java.util.Collections;
import java.util.List;

/** Auditable adaptive plan change at a completed stage boundary. */
public final class RetrievalReplanEvent {
    private final String boundary;
    private final String reason;
    private final RetrievalPlanSummary oldPlan;
    private final RetrievalPlanSummary newPlan;
    private final int remainingCandidates;
    private final int remainingTokens;
    private final long remainingLatencyNanos;
    private final int unusedCandidates;
    private final int unusedTokens;
    private final List<String> adjustments;
    private final String fallback;
    private final double firstStageScoreGap;
    private final int anchorCount;
    private final double anchorConfidence;
    private final int candidateDegree;
    private final double graphGrowthRate;
    private final double graphMarginalYield;

    public RetrievalReplanEvent(String boundary, String reason, RetrievalPlan oldPlan,
                                RetrievalPlan newPlan, RetrievalProgress progress,
                                int unusedCandidates, int unusedTokens,
                                List<String> adjustments, String fallback) {
        this.boundary = boundary;
        this.reason = reason;
        this.oldPlan = new RetrievalPlanSummary(oldPlan);
        this.newPlan = new RetrievalPlanSummary(newPlan);
        this.remainingCandidates = progress.getRemainingCandidates();
        this.remainingTokens = progress.getRemainingTokens();
        this.remainingLatencyNanos = progress.getRemainingLatencyNanos();
        this.unusedCandidates = unusedCandidates;
        this.unusedTokens = unusedTokens;
        this.adjustments = Collections.unmodifiableList(adjustments == null
            ? new ArrayList<String>() : new ArrayList<>(adjustments));
        this.fallback = fallback;
        this.firstStageScoreGap = progress.getFirstStageScoreGap();
        this.anchorCount = progress.getAnchorCount();
        this.anchorConfidence = progress.getAnchorConfidence();
        this.candidateDegree = progress.getCandidateDegree();
        this.graphGrowthRate = progress.getGraphGrowthRate();
        this.graphMarginalYield = progress.getGraphMarginalYield();
    }

    public String getBoundary() {
        return boundary;
    }

    public String getReason() {
        return reason;
    }

    public RetrievalPlanSummary getOldPlan() {
        return oldPlan;
    }

    public RetrievalPlanSummary getNewPlan() {
        return newPlan;
    }

    public int getRemainingCandidates() {
        return remainingCandidates;
    }

    public int getRemainingTokens() {
        return remainingTokens;
    }

    public long getRemainingLatencyNanos() {
        return remainingLatencyNanos;
    }

    public int getUnusedCandidates() {
        return unusedCandidates;
    }

    public int getUnusedTokens() {
        return unusedTokens;
    }

    public List<String> getAdjustments() {
        return adjustments;
    }

    public String getFallback() {
        return fallback;
    }

    public double getFirstStageScoreGap() {
        return firstStageScoreGap;
    }

    public int getAnchorCount() {
        return anchorCount;
    }

    public double getAnchorConfidence() {
        return anchorConfidence;
    }

    public int getCandidateDegree() {
        return candidateDegree;
    }

    public double getGraphGrowthRate() {
        return graphGrowthRate;
    }

    public double getGraphMarginalYield() {
        return graphMarginalYield;
    }
}
