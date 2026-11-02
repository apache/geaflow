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
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Reproducible threshold policy for the first adaptive routing implementation. */
public final class DeterministicRulePolicy implements RetrievalRoutingPolicy {
    public static final String POLICY_ID = "deterministic-rule";
    public static final String POLICY_VERSION = "v1";

    /** Lexical-leaning fusion weights applied when BM25 looks confident for this query. */
    static final double LEXICAL_BM25_WEIGHT = 1.25D;
    static final double LEXICAL_VECTOR_WEIGHT = 0.85D;
    /** Semantic-leaning fusion weights applied for long, lexically weak queries. */
    static final double SEMANTIC_BM25_WEIGHT = 0.85D;
    static final double SEMANTIC_VECTOR_WEIGHT = 1.25D;

    private final double highAverageIdf;
    private final double highScoreGap;
    private final int longQueryLength;
    private final double strongAnchorConfidence;
    private final int minimumAnchorCount;
    private final int highCandidateDegree;
    private final double highGrowthRate;
    private final int baseNeighborCap;
    private final Map<String, String> parameters;

    public DeterministicRulePolicy() {
        this(1.5D, 0.20D, 12, 0.75D, 1, 32, 2.0D, 16);
    }

    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, int baseNeighborCap) {
        if (!finiteNonNegative(highAverageIdf) || !finiteNonNegative(highScoreGap)
            || longQueryLength < 1 || !finiteRange(strongAnchorConfidence)
            || minimumAnchorCount < 1 || highCandidateDegree < 1
            || !finiteNonNegative(highGrowthRate) || baseNeighborCap < 1) {
            throw new IllegalArgumentException("invalid deterministic routing thresholds");
        }
        this.highAverageIdf = highAverageIdf;
        this.highScoreGap = highScoreGap;
        this.longQueryLength = longQueryLength;
        this.strongAnchorConfidence = strongAnchorConfidence;
        this.minimumAnchorCount = minimumAnchorCount;
        this.highCandidateDegree = highCandidateDegree;
        this.highGrowthRate = highGrowthRate;
        this.baseNeighborCap = baseNeighborCap;
        Map<String, String> values = new LinkedHashMap<>();
        values.put("highAverageIdf", Double.toString(highAverageIdf));
        values.put("highScoreGap", Double.toString(highScoreGap));
        values.put("longQueryLength", Integer.toString(longQueryLength));
        values.put("strongAnchorConfidence", Double.toString(strongAnchorConfidence));
        values.put("minimumAnchorCount", Integer.toString(minimumAnchorCount));
        values.put("highCandidateDegree", Integer.toString(highCandidateDegree));
        values.put("highGrowthRate", Double.toString(highGrowthRate));
        values.put("baseNeighborCap", Integer.toString(baseNeighborCap));
        parameters = Collections.unmodifiableMap(values);
    }

    private static boolean finiteNonNegative(double value) {
        return Double.isFinite(value) && value >= 0.0D;
    }

    private static boolean finiteRange(double value) {
        return Double.isFinite(value) && value >= 0.0D && value <= 1.0D;
    }

    @Override
    public String getPolicyId() {
        return POLICY_ID;
    }

    @Override
    public String getPolicyVersion() {
        return POLICY_VERSION;
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public RetrievalPlan plan(RoutingContext context, RetrievalProgress progress) {
        if (context == null) {
            throw new IllegalArgumentException("routing context is required");
        }
        RetrievalMode requested = context.getRequestedMode().canonical();
        if (requested != RetrievalMode.HYBRID) {
            return singleChannelPlan(context, requested);
        }
        boolean replanExhausted = progress != null && context.getMaxReplanCount() > 0
            && progress.getReplanCount() >= context.getMaxReplanCount();
        RoutingFeatures features = progress == null || replanExhausted ? context.getFeatures()
            : mergedFeatures(context, progress);
        RetrievalBudget budget = context.getBudget();
        boolean bm25 = context.hasIndex("BM25");
        boolean vector = context.hasIndex("VECTOR");
        // A graph index remains useful as a bounded fallback even when entity
        // linking is weak.  Anchor confidence controls hop/neighbor limits;
        // growth remains the hard stop that can disable the channel.
        boolean graph = context.hasIndex("GRAPH") && features.getGraphGrowthRate() <= highGrowthRate;
        if (!bm25 && !vector && !graph) {
            throw new IllegalArgumentException("no retrieval index is available");
        }
        if (!bm25 || !vector) {
            RetrievalMode fallback = bm25 ? RetrievalMode.BM25_ONLY
                : vector ? RetrievalMode.VECTOR_ONLY : RetrievalMode.GRAPH_ONLY;
            String channel = bm25 ? "BM25" : vector ? "VECTOR" : "GRAPH";
            int candidates = context.getBudget().getMaxCandidates();
            return RetrievalPlan.builder().selectedMode(fallback).executionMode(context.getExecutionMode())
                .topK(context.getBudget().getTopK()).candidateBudget(candidates)
                .tokenBudget(context.getBudget().getTokenBudget())
                .latencyBudgetNanos(context.getBudget().getTimeoutMs().longValue() * 1_000_000L)
                .channelBudget(channel, candidates).channelOrder(Collections.singletonList(channel))
                .maxHop("GRAPH".equals(channel) ? Math.min(1, context.getMaxHop()) : 0)
                .maxDiffusionGrowth(highGrowthRate)
                .neighborCap(neighborCap(candidates, features.getCandidateDegree()))
                .policyId(POLICY_ID).policyVersion(POLICY_VERSION)
                .reason("FALLBACK_MISSING_HYBRID_INDEX").build();
        }
        RetrievalPlan.Builder builder = RetrievalPlan.builder()
            .selectedMode(RetrievalMode.HYBRID).executionMode(context.getExecutionMode())
            .topK(budget.getTopK()).candidateBudget(budget.getMaxCandidates())
            .tokenBudget(budget.getTokenBudget())
            .latencyBudgetNanos(budget.getTimeoutMs().longValue() * 1_000_000L)
            .maxHop(graph ? graphMaxHop(context, features) : 0)
            .neighborCap(neighborCap(budget.getMaxCandidates(), features.getCandidateDegree()))
            .maxDiffusionGrowth(highGrowthRate)
            .policyId(POLICY_ID).policyVersion(POLICY_VERSION)
            .hardUpperBound("candidates", budget.getMaxCandidates())
            .hardUpperBound("tokens", budget.getTokenBudget());
        if (!bm25 || !vector) {
            builder.reason(!bm25 ? "BM25_UNAVAILABLE" : "VECTOR_UNAVAILABLE");
        }
        if (!graph) {
            builder.reason(features.getGraphGrowthRate() > highGrowthRate
                ? "GRAPH_STOPPED_GROWTH" : "GRAPH_ANCHOR_OR_INDEX_UNAVAILABLE");
        } else if (features.getAnchorCount() < minimumAnchorCount
            || features.getAnchorConfidence() < strongAnchorConfidence) {
            builder.reason("GRAPH_BOUNDED_ANCHOR_FALLBACK");
        }
        if (features.getAverageIdf() >= highAverageIdf
            || features.getFirstStageScoreGap() >= highScoreGap) {
            builder.bm25Weight(LEXICAL_BM25_WEIGHT).vectorWeight(LEXICAL_VECTOR_WEIGHT)
                .reason("LEXICAL_CONFIDENCE");
        } else if (features.getQueryLength() >= longQueryLength) {
            builder.bm25Weight(SEMANTIC_BM25_WEIGHT).vectorWeight(SEMANTIC_VECTOR_WEIGHT)
                .reason("SEMANTIC_QUERY");
        } else {
            builder.reason("BALANCED_QUERY");
        }
        addChannels(builder, bm25, vector, graph, features, budget.getMaxCandidates(), budget.getTopK(),
            progress != null && "AFTER_TEXT".equals(progress.getBoundary()), replanExhausted);
        return builder.build();
    }

    private RetrievalPlan singleChannelPlan(RoutingContext context, RetrievalMode requested) {
        String channel = requested == RetrievalMode.BM25_ONLY ? "BM25"
            : requested == RetrievalMode.VECTOR_ONLY ? "VECTOR" : "GRAPH";
        if (!context.hasIndex(channel)) {
            RetrievalMode fallback = context.hasIndex("BM25") ? RetrievalMode.BM25_ONLY
                : context.hasIndex("VECTOR") ? RetrievalMode.VECTOR_ONLY : RetrievalMode.GRAPH_ONLY;
            if (!context.hasIndex(channel) && !context.hasIndex(fallback == RetrievalMode.BM25_ONLY
                ? "BM25" : fallback == RetrievalMode.VECTOR_ONLY ? "VECTOR" : "GRAPH")) {
                throw new IllegalArgumentException("requested retrieval index is unavailable: " + channel);
            }
            requested = fallback;
            channel = requested == RetrievalMode.BM25_ONLY ? "BM25"
                : requested == RetrievalMode.VECTOR_ONLY ? "VECTOR" : "GRAPH";
        }
        RetrievalBudget budget = context.getBudget();
        return RetrievalPlan.builder().selectedMode(requested).executionMode(context.getExecutionMode())
            .topK(budget.getTopK()).candidateBudget(budget.getMaxCandidates())
            .tokenBudget(budget.getTokenBudget())
            .latencyBudgetNanos(budget.getTimeoutMs().longValue() * 1_000_000L)
            .maxHop("GRAPH".equals(channel) ? Math.min(1, context.getMaxHop()) : 0)
            .neighborCap(neighborCap(budget.getMaxCandidates(), context.getFeatures().getCandidateDegree()))
            .channelBudget(channel, budget.getMaxCandidates()).channelOrder(Collections.singletonList(channel))
            .fallbackMode(requested == context.getRequestedMode().canonical() ? null : requested)
            .policyId(POLICY_ID).policyVersion(POLICY_VERSION).reason("EXPLICIT_MODE").build();
    }

    private void addChannels(RetrievalPlan.Builder builder, boolean bm25, boolean vector, boolean graph,
                             RoutingFeatures features, int candidateBudget, int topK, boolean afterText,
                             boolean replanExhausted) {
        if (replanExhausted) {
            builder.reason("REPLAN_BUDGET_EXHAUSTED");
        }
        int selectedCount = (bm25 ? 1 : 0) + (vector ? 1 : 0) + (graph ? 1 : 0);
        if (candidateBudget < selectedCount) {
            throw new IllegalArgumentException("candidate budget cannot fund every selected channel");
        }
        if (afterText && graph) {
            int graphBudget = candidateBudget - (bm25 ? 1 : 0) - (vector ? 1 : 0);
            if (bm25) {
                builder.channelBudget("BM25", 1);
            }
            if (vector) {
                builder.channelBudget("VECTOR", 1);
            }
            builder.channelBudget("GRAPH", graphBudget);
            order(builder, bm25, vector, true, "BM25");
            return;
        }
        int textChannelBudget = Math.max(10, topK);
        if (graph && bm25 && vector && candidateBudget > 2 * textChannelBudget + selectedCount) {
            int textBudget = 2 * textChannelBudget;
            int baseTextBudget = textBudget / 2;
            int textRemainder = textBudget % 2;
            if (bm25) {
                builder.channelBudget("BM25", baseTextBudget + (textRemainder-- > 0 ? 1 : 0));
            }
            if (vector) {
                builder.channelBudget("VECTOR", baseTextBudget + (textRemainder > 0 ? 1 : 0));
            }
            builder.channelBudget("GRAPH", candidateBudget - textBudget);
            order(builder, bm25, vector, true, features.getQueryLength() >= longQueryLength ? "VECTOR" : "BM25");
            return;
        }
        int base = candidateBudget / selectedCount;
        int remainder = candidateBudget % selectedCount;
        int bm25Budget = bm25 ? base : 0;
        int vectorBudget = vector ? base : 0;
        int graphBudget = graph ? base : 0;
        if (remainder > 0 && bm25) {
            bm25Budget++;
            remainder--;
        }
        if (remainder > 0 && vector) {
            vectorBudget++;
            remainder--;
        }
        if (remainder > 0 && graph) {
            graphBudget++;
        }
        remainder = 0;
        while (remainder > 0) {
            if (bm25 && remainder > 0) {
                bm25Budget++;
                remainder--;
            }
            if (vector && remainder > 0) {
                vectorBudget++;
                remainder--;
            }
            if (graph && remainder > 0) {
                graphBudget++;
                remainder--;
            }
        }
        if (bm25) {
            builder.channelBudget("BM25", bm25Budget);
        }
        if (vector) {
            builder.channelBudget("VECTOR", vectorBudget);
        }
        if (graph) {
            builder.channelBudget("GRAPH", graphBudget);
        }
        if (features.getAverageIdf() >= highAverageIdf
            || features.getFirstStageScoreGap() >= highScoreGap) {
            order(builder, bm25, vector, graph, "BM25");
        } else if (features.getQueryLength() >= longQueryLength) {
            order(builder, bm25, vector, graph, "VECTOR");
        } else {
            order(builder, bm25, vector, graph, "BM25");
        }
    }

    private static void order(RetrievalPlan.Builder builder, boolean bm25, boolean vector, boolean graph,
                              String first) {
        if ("BM25".equals(first) && bm25) {
            builder.channelOrder(java.util.Arrays.asList("BM25", vector ? "VECTOR" : null, graph ? "GRAPH" : null));
        } else if (vector) {
            builder.channelOrder(java.util.Arrays.asList("VECTOR", bm25 ? "BM25" : null, graph ? "GRAPH" : null));
        } else if (bm25) {
            builder.channelOrder(java.util.Arrays.asList("BM25", graph ? "GRAPH" : null));
        } else {
            builder.channelOrder(Collections.singletonList("GRAPH"));
        }
    }

    private int graphMaxHop(RoutingContext context, RoutingFeatures features) {
        boolean strong = features.getAnchorCount() >= minimumAnchorCount
            && features.getAnchorConfidence() >= strongAnchorConfidence
            && features.getCandidateDegree() <= highCandidateDegree;
        return strong ? Math.min(2, context.getMaxHop()) : Math.min(1, context.getMaxHop());
    }

    private int neighborCap(int candidateBudget, int candidateDegree) {
        if (candidateDegree > highCandidateDegree) {
            return Math.max(1, Math.min(baseNeighborCap, candidateBudget / 2));
        }
        return Math.max(1, Math.min(baseNeighborCap, candidateBudget));
    }

    private RoutingFeatures mergedFeatures(RoutingContext context, RetrievalProgress progress) {
        return RoutingFeatures.builder().queryLength(context.getFeatures().getQueryLength())
            .averageIdf(context.getFeatures().getAverageIdf())
            .firstStageScoreGap(progress.getFirstStageScoreGap())
            .anchorCount(progress.getAnchorCount()).anchorConfidence(progress.getAnchorConfidence())
            .candidateDegree(progress.getCandidateDegree()).graphGrowthRate(progress.getGraphGrowthRate())
            .build();
    }
}
