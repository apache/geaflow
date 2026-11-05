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
public final class DeterministicRulePolicy implements AdaptivePolicy {
    public static final String POLICY_ID = "deterministic-rule";
    public static final String POLICY_VERSION = "v1";

    /** Lexical-leaning fusion weights applied when BM25 looks confident for this query. */
    static final double LEXICAL_BM25_WEIGHT = 1.25D;
    static final double LEXICAL_VECTOR_WEIGHT = 0.85D;
    /** Semantic-leaning fusion weights applied for long, lexically weak queries. */
    static final double SEMANTIC_BM25_WEIGHT = 0.85D;
    static final double SEMANTIC_VECTOR_WEIGHT = 1.25D;
    /**
     * Fusion weights used when text channels show no reliable lexical or semantic signal.
     *
     * <p>The measured end-to-end runs put BM25 and vector Recall@10 at {@code 0.002}: text channels
     * almost never surface the gold supporting chunk, while graph expansion supplies it. Weighting
     * a channel that carries no signal still adds its rank term to every fused score, which is
     * exactly what pushes correct graph evidence below noisy text evidence and collapses MRR. When
     * the probe shows no lexical confidence the graph channel therefore leads and the text
     * channels are demoted rather than promoted.
     */
    static final double GRAPH_FIRST_GRAPH_WEIGHT = 2.0D;
    static final double GRAPH_FIRST_TEXT_WEIGHT = 0.25D;
    static final double DEFAULT_HIGH_AVERAGE_IDF = 1.5D;
    /**
     * Default first-stage score-gap threshold.
     *
     * <p>Matches {@code RetrievalProperties}. The earlier constructor default of {@code 0.20} was
     * unreachable for a reciprocal-rank score, whose gap stays inside {@code [0, 0.003]}.
     */
    static final double DEFAULT_HIGH_SCORE_GAP = 0.0005D;
    static final int DEFAULT_LONG_QUERY_LENGTH = 12;
    static final double DEFAULT_STRONG_ANCHOR_CONFIDENCE = 0.75D;
    static final int DEFAULT_MINIMUM_ANCHOR_COUNT = 1;
    static final int DEFAULT_HIGH_CANDIDATE_DEGREE = 32;
    static final double DEFAULT_HIGH_GROWTH_RATE = 2.0D;
    static final int DEFAULT_BASE_NEIGHBOR_CAP = 16;
    /**
     * Largest share of the candidate budget the text probe may consume before graph expansion.
     *
     * <p>{@code 0.4} keeps a 50-candidate request at a 30-candidate graph budget (versus the
     * 50 graph-only receives and the 16 the previous split produced) while still funding the probe.
     */
    static final double GRAPH_PROBE_SHARE = 0.4D;

    private final double highAverageIdf;
    private final double highScoreGap;
    private final int longQueryLength;
    private final double strongAnchorConfidence;
    private final int minimumAnchorCount;
    private final int highCandidateDegree;
    private final double highGrowthRate;
    private final int baseNeighborCap;
    private final double graphWeight;
    private final double textWeight;
    private final double probeShare;
    private final double lexicalBm25Weight;
    private final double lexicalVectorWeight;
    private final double semanticBm25Weight;
    private final double semanticVectorWeight;
    private final Map<String, String> parameters;

    public DeterministicRulePolicy() {
        this(DEFAULT_HIGH_AVERAGE_IDF, DEFAULT_HIGH_SCORE_GAP, DEFAULT_LONG_QUERY_LENGTH,
            DEFAULT_STRONG_ANCHOR_CONFIDENCE, DEFAULT_MINIMUM_ANCHOR_COUNT,
            DEFAULT_HIGH_CANDIDATE_DEGREE, DEFAULT_HIGH_GROWTH_RATE, DEFAULT_BASE_NEIGHBOR_CAP);
    }

    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, int baseNeighborCap) {
        this(highAverageIdf, highScoreGap, longQueryLength, strongAnchorConfidence, minimumAnchorCount,
            highCandidateDegree, highGrowthRate, baseNeighborCap,
            GRAPH_FIRST_GRAPH_WEIGHT, GRAPH_FIRST_TEXT_WEIGHT, GRAPH_PROBE_SHARE,
            LEXICAL_BM25_WEIGHT, LEXICAL_VECTOR_WEIGHT, SEMANTIC_BM25_WEIGHT, SEMANTIC_VECTOR_WEIGHT);
    }

    /**
     * Creates a policy with every threshold and fusion weight supplied by configuration.
     *
     * <p>All of these were constants in the first revision. The fusion weights and the probe share
     * decide how much of the score and budget the graph channel receives, so they are the most
     * quality-sensitive values in the policy: demoting the text channels from {@code 1.25} to
     * {@code 0.25} and raising the graph channel to {@code 2.0} moved 2Wiki MRR from {@code 0.1019}
     * to {@code 0.4747}. Hard-coding them made that comparison impossible to rerun under a
     * different configuration.
     *
     * @param highAverageIdf average query IDF above which the query counts as lexically rich
     * @param highScoreGap first-stage score gap above which a probe is trusted
     * @param longQueryLength query length at which a query counts as a long semantic question
     * @param strongAnchorConfidence anchor confidence required to treat an anchor as reliable
     * @param minimumAnchorCount anchors required to treat an anchor set as reliable
     * @param highCandidateDegree average degree above which a frontier counts as high-degree
     * @param highGrowthRate graph growth rate above which expansion stops
     * @param baseNeighborCap baseline neighbours sampled per vertex
     * @param graphWeight fusion weight for the graph channel when it leads
     * @param textWeight fusion weight for each text channel when the graph channel leads
     * @param probeShare largest share of the candidate budget the text probe may take
     * @param lexicalBm25Weight BM25 weight when the query shows lexical confidence
     * @param lexicalVectorWeight vector weight when the query shows lexical confidence
     * @param semanticBm25Weight BM25 weight for a long semantic query
     * @param semanticVectorWeight vector weight for a long semantic query
     */
    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, int baseNeighborCap,
                                   double graphWeight, double textWeight, double probeShare,
                                   double lexicalBm25Weight, double lexicalVectorWeight,
                                   double semanticBm25Weight, double semanticVectorWeight) {
        if (!finiteNonNegative(highAverageIdf) || !finiteNonNegative(highScoreGap)
            || longQueryLength < 1 || !finiteRange(strongAnchorConfidence)
            || minimumAnchorCount < 1 || highCandidateDegree < 1
            || !finiteNonNegative(highGrowthRate) || baseNeighborCap < 1) {
            throw new IllegalArgumentException("invalid deterministic routing thresholds");
        }
        if (!finiteNonNegative(graphWeight) || !finiteNonNegative(textWeight)
            || !finiteNonNegative(lexicalBm25Weight) || !finiteNonNegative(lexicalVectorWeight)
            || !finiteNonNegative(semanticBm25Weight) || !finiteNonNegative(semanticVectorWeight)
            || (graphWeight == 0.0D && textWeight == 0.0D)
            || !Double.isFinite(probeShare) || probeShare < 0.0D || probeShare >= 1.0D) {
            throw new IllegalArgumentException("invalid deterministic fusion weights");
        }
        this.highAverageIdf = highAverageIdf;
        this.highScoreGap = highScoreGap;
        this.longQueryLength = longQueryLength;
        this.strongAnchorConfidence = strongAnchorConfidence;
        this.minimumAnchorCount = minimumAnchorCount;
        this.highCandidateDegree = highCandidateDegree;
        this.highGrowthRate = highGrowthRate;
        this.baseNeighborCap = baseNeighborCap;
        this.graphWeight = graphWeight;
        this.textWeight = textWeight;
        this.probeShare = probeShare;
        this.lexicalBm25Weight = lexicalBm25Weight;
        this.lexicalVectorWeight = lexicalVectorWeight;
        this.semanticBm25Weight = semanticBm25Weight;
        this.semanticVectorWeight = semanticVectorWeight;
        Map<String, String> values = new LinkedHashMap<>();
        values.put("highAverageIdf", Double.toString(highAverageIdf));
        values.put("highScoreGap", Double.toString(highScoreGap));
        values.put("longQueryLength", Integer.toString(longQueryLength));
        values.put("strongAnchorConfidence", Double.toString(strongAnchorConfidence));
        values.put("minimumAnchorCount", Integer.toString(minimumAnchorCount));
        values.put("highCandidateDegree", Integer.toString(highCandidateDegree));
        values.put("highGrowthRate", Double.toString(highGrowthRate));
        values.put("baseNeighborCap", Integer.toString(baseNeighborCap));
        // Recorded so a replayed plan can be checked against the weights that produced it.
        values.put("graphFusionGraphWeight", Double.toString(graphWeight));
        values.put("graphFusionTextWeight", Double.toString(textWeight));
        values.put("graphProbeShare", Double.toString(probeShare));
        values.put("lexicalBm25Weight", Double.toString(lexicalBm25Weight));
        values.put("lexicalVectorWeight", Double.toString(lexicalVectorWeight));
        values.put("semanticBm25Weight", Double.toString(semanticBm25Weight));
        values.put("semanticVectorWeight", Double.toString(semanticVectorWeight));
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
    public RetrievalPlan plan(RoutingContext context) {
        return createPlan(context, null);
    }

    @Override
    public boolean supports(org.apache.geaflow.ai.retrieval.api.model.RetrievalMode mode) {
        return mode != null;
    }

    /**
     * Returns the plan revised at a stage boundary.
     *
     * <p>The design allows a policy to shrink or abandon graph expansion once probe statistics
     * exist. This rule policy re-runs the same decision with the merged features.
     */
    @Override
    public RetrievalPlan replan(RoutingContext context, RetrievalProgress progress) {
        return createPlan(context, progress);
    }

    private RetrievalPlan createPlan(RoutingContext context, RetrievalProgress progress) {
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
        // Average query IDF is not retrieval confidence, and it must not demote the graph channel.
        //
        // The IDF feature is computed from the raw query before any channel runs, so it reports that
        // the query is built from rare terms - not that BM25 matched them. It also correlates with
        // the wrong thing: on the measured 2Wiki run the IDF median is 3.0 and the anchor
        // confidence median is 1.0, so an IDF-driven lexical branch fired on 494 of 500 queries -
        // exactly the queries where an entity was reliably grounded and the graph channel had the
        // most to offer - while BM25 recall stayed at 0.002. Ranking a channel that carries no
        // signal still adds its rank term to every fused score, which is what pushed correct graph
        // evidence below noisy text evidence and collapsed MRR.
        //
        // The branch is therefore driven by the evidence that exists rather than by query shape:
        // a confident anchor means the graph channel has a seed it can expand, so it leads. Only the
        // absence of an anchor falls back to the text channels.
        boolean anchored = features.getAnchorCount() >= minimumAnchorCount
            && features.getAnchorConfidence() >= strongAnchorConfidence;
        if (anchored) {
            // A grounded entity is the strongest available retrieval signal: let graph expansion
            // lead the fusion while the text channels supply corroborating rank evidence.
            builder.bm25Weight(textWeight).vectorWeight(textWeight)
                .graphWeight(graphWeight)
                .reason("GRAPH_DOMINANT_FUSION");
        } else if (features.getAverageIdf() >= highAverageIdf) {
            builder.bm25Weight(lexicalBm25Weight).vectorWeight(lexicalVectorWeight)
                .graphWeight(1.0D)
                .reason("LEXICAL_CONFIDENCE");
        } else if (features.getQueryLength() >= longQueryLength) {
            builder.bm25Weight(semanticBm25Weight).vectorWeight(semanticVectorWeight)
                .graphWeight(1.0D)
                .reason("SEMANTIC_QUERY");
        } else {
            builder.bm25Weight(textWeight).vectorWeight(textWeight)
                .graphWeight(graphWeight)
                .reason("GRAPH_DOMINANT_FUSION");
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
        // Once the replan budget is spent the policy must stop moving the allocation. Re-applying
        // the after-text split here still produced a new plan, so a policy that had reached
        // maxReplanCount kept handing the graph stage a different budget than the one already
        // recorded - the loop the bound exists to prevent.
        if (afterText && graph && !replanExhausted) {
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
            // Text channels get a probe-sized share; the graph channel receives the remainder.
            //
            // The previous split reserved a fixed 2 * max(10, topK) for text and handed the graph
            // channel whatever was left. With a 50-candidate budget that game the graph channel 16
            // candidates while graph-only received 50, and the measured runs show the graph channel
            // is the one that supplies evidence. The probe share is therefore capped so the graph
            // channel cannot be starved below the graph-only allocation it is being compared against.
            int graphFirstBudget = graphFirstBudget(candidateBudget, selectedCount);
            int textBudget = candidateBudget - graphFirstBudget;
            int baseTextBudget = textBudget / 2;
            int textRemainder = textBudget % 2;
            if (bm25) {
                builder.channelBudget("BM25", baseTextBudget + (textRemainder-- > 0 ? 1 : 0));
            }
            if (vector) {
                builder.channelBudget("VECTOR", baseTextBudget + (textRemainder > 0 ? 1 : 0));
            }
            builder.channelBudget("GRAPH", graphFirstBudget);
            order(builder, bm25, vector, true, features.getQueryLength() >= longQueryLength ? "VECTOR" : "BM25");
            return;
        }
        // Budgets too small for the graph-first split still have to respect the same ordering rule:
        // the graph channel never receives less than a text channel. The previous even split awarded
        // the remainder to BM25 and VECTOR first, which at a 10-candidate budget left the graph
        // channel with 3 against BM25's 4 - the exact inversion that starves the only channel that
        // supplies evidence.
        int base = candidateBudget / selectedCount;
        int remainder = candidateBudget % selectedCount;
        int bm25Budget = bm25 ? base : 0;
        int vectorBudget = vector ? base : 0;
        int graphBudget = graph ? base : 0;
        while (remainder > 0) {
            if (graph && remainder > 0) {
                graphBudget++;
                remainder--;
            }
            if (bm25 && remainder > 0) {
                bm25Budget++;
                remainder--;
            }
            if (vector && remainder > 0) {
                vectorBudget++;
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

    /**
     * Returns the candidate budget reserved for the graph channel in a full-hybrid plan.
     *
     * <p>The probe still receives enough candidates to resolve anchors and to produce the score-gap
     * statistics the policy consumes, but it may never take more than
     * configured probe share of the budget. Everything else goes to the channel that the
     * measured runs show actually supplies evidence.
     *
     * @param candidateBudget total candidate budget for the request
     * @param selectedCount number of channels scheduled
     * @return the graph channel budget, always at least one candidate
     */
    private int graphFirstBudget(int candidateBudget, int selectedCount) {
        int probe = (int) Math.floor(candidateBudget * probeShare);
        int graphBudget = candidateBudget - probe;
        return Math.max(selectedCount, Math.max(1, graphBudget));
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
