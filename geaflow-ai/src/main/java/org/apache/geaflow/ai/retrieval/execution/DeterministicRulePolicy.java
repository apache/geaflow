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
    /** Reported when the decoupled text/anchor routing is enabled, so traces stay comparable. */
    public static final String POLICY_VERSION_DECOUPLED = "v2";

    /** Lexical-leaning fusion weights applied when BM25 looks confident for this query. */
    static final double LEXICAL_BM25_WEIGHT = 1.25D;
    static final double LEXICAL_VECTOR_WEIGHT = 0.85D;
    /** Semantic-leaning fusion weights applied for long, lexically weak queries. */
    static final double SEMANTIC_BM25_WEIGHT = 0.85D;
    static final double SEMANTIC_VECTOR_WEIGHT = 1.25D;
    /**
     * Default fusion weights for the anchored and no-confidence branches.
     *
     * <p>Only the ratio reaches reciprocal-rank fusion. It used to be 8:1 in favour of the graph
     * channel, justified by runs whose text recall of {@code 0.002} came from scoring chunk ids
     * that did not exist in the corpus. On the corrected bundles the text channels are the stronger
     * ones (2Wiki BM25 MRR 0.7368 against graph 0.5260; HotpotQA 0.8254 against 0.3272), and the
     * 8:1 prior cost 7.8% of MRR on 2Wiki and 15.0% on HotpotQA against fixed hybrid.
     *
     * <p>A 200-query sweep per dataset ({@code tools/graphrag/sweep_fusion.sh}) puts the optimum at
     * 1:4 on both: adaptive MRR moves from 7.8% behind fixed hybrid to 2.6% ahead on 2Wiki, and
     * from 15.0% behind to 0.1% behind on HotpotQA. Ratios in {@code [0, 1]} form a plateau within
     * noise, so the graph channel still contributes rank evidence - it just no longer outweighs the
     * channels that retrieve better. {@code GRAPH_DOMINANT_FUSION} is kept as the reason code of
     * that branch so existing traces remain comparable; it no longer implies a dominant weight.
     */
    static final double DEFAULT_GRAPH_FUSION_WEIGHT = 0.25D;
    static final double DEFAULT_TEXT_FUSION_WEIGHT = 1.0D;
    /**
     * Default lexical-confidence threshold, matched to {@code RetrievalProperties}.
     *
     * <p>Set near the 90th percentile of the measured query-IDF distribution rather than below its
     * minimum: the previous 1.5 admitted every 2Wiki query and so measured nothing.
     */
    static final double DEFAULT_HIGH_AVERAGE_IDF = 4.0D;
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
    /**
     * Default marginal-yield stop threshold.
     *
     * <p>Zero disables the stop, matching the graph retriever's documented default. The value is
     * kept as a named threshold so a benchmark can pin it in the policy parameters.
     */
    static final double DEFAULT_MIN_MARGINAL_YIELD = 0.0D;
    static final int DEFAULT_BASE_NEIGHBOR_CAP = 16;
    /** Stage boundaries the policy distinguishes. */
    static final String BOUNDARY_AFTER_TEXT = "AFTER_TEXT";
    static final String BOUNDARY_AFTER_TEXT_PROBE = "AFTER_TEXT_PROBE";
    static final String BOUNDARY_AFTER_GRAPH_HOP = "AFTER_GRAPH_HOP";
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
    private final double minMarginalYield;
    private final int baseNeighborCap;
    private final double graphWeight;
    private final double textWeight;
    private final double probeShare;
    private final double lexicalBm25Weight;
    private final double lexicalVectorWeight;
    private final double semanticBm25Weight;
    private final double semanticVectorWeight;
    private final boolean decoupleTextRouting;
    private final Map<String, String> parameters;

    public DeterministicRulePolicy() {
        this(DEFAULT_HIGH_AVERAGE_IDF, DEFAULT_HIGH_SCORE_GAP, DEFAULT_LONG_QUERY_LENGTH,
            DEFAULT_STRONG_ANCHOR_CONFIDENCE, DEFAULT_MINIMUM_ANCHOR_COUNT,
            DEFAULT_HIGH_CANDIDATE_DEGREE, DEFAULT_HIGH_GROWTH_RATE, DEFAULT_MIN_MARGINAL_YIELD,
            DEFAULT_BASE_NEIGHBOR_CAP);
    }

    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, int baseNeighborCap) {
        this(highAverageIdf, highScoreGap, longQueryLength, strongAnchorConfidence, minimumAnchorCount,
            highCandidateDegree, highGrowthRate, DEFAULT_MIN_MARGINAL_YIELD, baseNeighborCap);
    }

    /**
     * Creates a policy whose marginal-yield stop threshold can be tuned independently.
     *
     * @param minMarginalYield new evidence per scanned edge below which expansion stops
     */
    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, double minMarginalYield,
                                   int baseNeighborCap) {
        this(highAverageIdf, highScoreGap, longQueryLength, strongAnchorConfidence, minimumAnchorCount,
            highCandidateDegree, highGrowthRate, minMarginalYield, baseNeighborCap,
            DEFAULT_GRAPH_FUSION_WEIGHT, DEFAULT_TEXT_FUSION_WEIGHT, GRAPH_PROBE_SHARE,
            LEXICAL_BM25_WEIGHT, LEXICAL_VECTOR_WEIGHT, SEMANTIC_BM25_WEIGHT, SEMANTIC_VECTOR_WEIGHT);
    }

    /**
     * Creates a policy with every threshold and fusion weight supplied by configuration, keeping the
     * marginal-yield stop at its documented default.
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
        this(highAverageIdf, highScoreGap, longQueryLength, strongAnchorConfidence, minimumAnchorCount,
            highCandidateDegree, highGrowthRate, DEFAULT_MIN_MARGINAL_YIELD, baseNeighborCap,
            graphWeight, textWeight, probeShare, lexicalBm25Weight, lexicalVectorWeight,
            semanticBm25Weight, semanticVectorWeight);
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
     * @param minMarginalYield new evidence per scanned edge below which expansion stops
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
                                   int highCandidateDegree, double highGrowthRate, double minMarginalYield,
                                   int baseNeighborCap,
                                   double graphWeight, double textWeight, double probeShare,
                                   double lexicalBm25Weight, double lexicalVectorWeight,
                                   double semanticBm25Weight, double semanticVectorWeight) {
        this(highAverageIdf, highScoreGap, longQueryLength, strongAnchorConfidence, minimumAnchorCount,
            highCandidateDegree, highGrowthRate, minMarginalYield, baseNeighborCap,
            graphWeight, textWeight, probeShare, lexicalBm25Weight, lexicalVectorWeight,
            semanticBm25Weight, semanticVectorWeight, false);
    }

    /**
     * Creates a policy with every threshold and fusion weight supplied by configuration, plus the
     * text/anchor decoupling switch.
     *
     * @param decoupleTextRouting when true, lexical and semantic features set the text-channel
     *        weights while the anchor only governs graph expansion, instead of the anchored branch
     *        short-circuiting both decisions
     */
    public DeterministicRulePolicy(double highAverageIdf, double highScoreGap, int longQueryLength,
                                   double strongAnchorConfidence, int minimumAnchorCount,
                                   int highCandidateDegree, double highGrowthRate, double minMarginalYield,
                                   int baseNeighborCap,
                                   double graphWeight, double textWeight, double probeShare,
                                   double lexicalBm25Weight, double lexicalVectorWeight,
                                   double semanticBm25Weight, double semanticVectorWeight,
                                   boolean decoupleTextRouting) {
        if (!finiteNonNegative(highAverageIdf) || !finiteNonNegative(highScoreGap)
            || longQueryLength < 1 || !finiteRange(strongAnchorConfidence)
            || minimumAnchorCount < 1 || highCandidateDegree < 1
            || !finiteNonNegative(highGrowthRate) || !finiteNonNegative(minMarginalYield)
            || baseNeighborCap < 1) {
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
        this.minMarginalYield = minMarginalYield;
        this.baseNeighborCap = baseNeighborCap;
        this.graphWeight = graphWeight;
        this.textWeight = textWeight;
        this.probeShare = probeShare;
        this.lexicalBm25Weight = lexicalBm25Weight;
        this.lexicalVectorWeight = lexicalVectorWeight;
        this.semanticBm25Weight = semanticBm25Weight;
        this.semanticVectorWeight = semanticVectorWeight;
        this.decoupleTextRouting = decoupleTextRouting;
        Map<String, String> values = new LinkedHashMap<>();
        values.put("highAverageIdf", Double.toString(highAverageIdf));
        values.put("highScoreGap", Double.toString(highScoreGap));
        values.put("longQueryLength", Integer.toString(longQueryLength));
        values.put("strongAnchorConfidence", Double.toString(strongAnchorConfidence));
        values.put("minimumAnchorCount", Integer.toString(minimumAnchorCount));
        values.put("highCandidateDegree", Integer.toString(highCandidateDegree));
        values.put("highGrowthRate", Double.toString(highGrowthRate));
        values.put("minMarginalYield", Double.toString(minMarginalYield));
        values.put("baseNeighborCap", Integer.toString(baseNeighborCap));
        // Recorded so a replayed plan can be checked against the weights that produced it.
        values.put("graphFusionGraphWeight", Double.toString(graphWeight));
        values.put("graphFusionTextWeight", Double.toString(textWeight));
        values.put("graphProbeShare", Double.toString(probeShare));
        values.put("lexicalBm25Weight", Double.toString(lexicalBm25Weight));
        values.put("lexicalVectorWeight", Double.toString(lexicalVectorWeight));
        values.put("semanticBm25Weight", Double.toString(semanticBm25Weight));
        values.put("semanticVectorWeight", Double.toString(semanticVectorWeight));
        // Recorded so a replayed plan can tell which routing structure produced it.
        values.put("decoupleTextRouting", Boolean.toString(decoupleTextRouting));
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
        return decoupleTextRouting ? POLICY_VERSION_DECOUPLED : POLICY_VERSION;
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
        // Measured features are merged whenever a stage has run, including once the replan budget
        // is spent. The budget bounds how often the allocation may move; it must not blind the
        // policy to the stop signals. Falling back to the pre-execution snapshot here made the
        // post-hop boundary re-plan from graphGrowthRate = 0, so a frontier that had already grown
        // past highGrowthRate was scheduled for another hop instead of being stopped.
        RoutingFeatures features = progress == null ? context.getFeatures()
            : mergedFeatures(context, progress);
        RetrievalBudget budget = context.getBudget();
        boolean bm25 = context.hasIndex("BM25");
        boolean vector = context.hasIndex("VECTOR");
        // A graph index remains useful as a bounded fallback even when entity
        // linking is weak.  Anchor confidence controls hop/neighbor limits;
        // growth remains the hard stop that can disable the channel. Growth is only meaningful once
        // a hop has actually been expanded, which is why the initial plan cannot use it and the
        // post-hop replan can.
        boolean growthStopped = progress != null && features.getGraphGrowthRate() > highGrowthRate;
        boolean graph = context.hasIndex("GRAPH") && !growthStopped;
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
                .policyId(POLICY_ID).policyVersion(getPolicyVersion())
                .reason("FALLBACK_MISSING_HYBRID_INDEX").build();
        }
        RetrievalPlan.Builder builder = RetrievalPlan.builder()
            .selectedMode(RetrievalMode.HYBRID).executionMode(context.getExecutionMode())
            .topK(budget.getTopK()).candidateBudget(budget.getMaxCandidates())
            .tokenBudget(budget.getTokenBudget())
            .latencyBudgetNanos(budget.getTimeoutMs().longValue() * 1_000_000L)
            .maxHop(graph ? graphMaxHop(context, features, progress, replanExhausted) : 0)
            .neighborCap(neighborCap(budget.getMaxCandidates(), features.getCandidateDegree()))
            .maxDiffusionGrowth(highGrowthRate)
            .policyId(POLICY_ID).policyVersion(getPolicyVersion())
            .hardUpperBound("candidates", budget.getMaxCandidates())
            .hardUpperBound("tokens", budget.getTokenBudget());
        if (!bm25 || !vector) {
            builder.reason(!bm25 ? "BM25_UNAVAILABLE" : "VECTOR_UNAVAILABLE");
        }
        if (!graph) {
            builder.reason(features.getGraphGrowthRate() > highGrowthRate
                ? "GRAPH_STOPPED_GROWTH" : "GRAPH_ANCHOR_OR_INDEX_UNAVAILABLE");
        } else if (yieldBelowThreshold(progress)) {
            // The hop that just ran produced too little new evidence per scanned edge. Spending the
            // next hop would grow the frontier without growing the answer, so expansion stops here
            // and the already-produced evidence is kept.
            builder.reason("MARGINAL_YIELD_BELOW_THRESHOLD");
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
        if (decoupleTextRouting) {
            applyDecoupledFusion(builder, features, anchored);
        } else if (anchored) {
            // A grounded entity is the strongest available retrieval signal: let graph expansion
            // lead the fusion while the text channels supply corroborating rank evidence.
            builder.bm25Weight(textWeight).vectorWeight(textWeight)
                .graphWeight(graphWeight)
                .reason("GRAPH_DOMINANT_FUSION");
        } else if (features.getAverageIdf() >= highAverageIdf) {
            // These branches re-balance the two text channels; they must not also raise the graph
            // weight. The hard-coded 1.0 quadrupled the graph rank contribution relative to the
            // swept default, so a rule that fired would have undone the fusion correction.
            builder.bm25Weight(lexicalBm25Weight).vectorWeight(lexicalVectorWeight)
                .graphWeight(graphWeight)
                .reason("LEXICAL_CONFIDENCE");
        } else if (features.getQueryLength() >= longQueryLength) {
            builder.bm25Weight(semanticBm25Weight).vectorWeight(semanticVectorWeight)
                .graphWeight(graphWeight)
                .reason("SEMANTIC_QUERY");
        } else {
            builder.bm25Weight(textWeight).vectorWeight(textWeight)
                .graphWeight(graphWeight)
                .reason("GRAPH_DOMINANT_FUSION");
        }
        addChannels(builder, bm25, vector, graph, features, budget.getMaxCandidates(), budget.getTopK(),
            textStageComplete(progress), replanExhausted);
        // Persist the stop rules the policy evaluated so the executor can apply exactly this plan
        // instead of re-reading global configuration the policy might have overridden.
        if (yieldBelowThreshold(progress)) {
            builder.minMarginalYield(Double.MAX_VALUE);
        }
        return builder.build();
    }

    /**
     * True when the hop that just ran yielded too little new evidence for its edge scans.
     *
     * <p>A zero or negative {@code minMarginalYield} disables the stop, matching the graph
     * retriever's own default. An unmeasured yield is zero and must not be mistaken for a measured
     * failure, so a request that never expanded a hop is exempt.
     */
    private boolean yieldBelowThreshold(RetrievalProgress progress) {
        return progress != null && minMarginalYield > 0.0D
            && BOUNDARY_AFTER_GRAPH_HOP.equals(progress.getBoundary())
            && progress.getGraphMarginalYield() < minMarginalYield;
    }

    /**
     * True when every text channel has already completed for this request.
     *
     * <p>The after-text budget split reserves a probe-sized share for BM25 and the vector channel and
     * hands the remainder to graph expansion. It must apply at the probe boundary and at the
     * post-hop boundary - both happen after the text stage - but not to the initial plan, which is
     * produced before any channel ran.
     */
    private static boolean textStageComplete(RetrievalProgress progress) {
        if (progress == null) {
            return false;
        }
        String boundary = progress.getBoundary();
        return BOUNDARY_AFTER_TEXT.equals(boundary)
            || BOUNDARY_AFTER_TEXT_PROBE.equals(boundary)
            || BOUNDARY_AFTER_GRAPH_HOP.equals(boundary);
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
            .policyId(POLICY_ID).policyVersion(getPolicyVersion()).reason("EXPLICIT_MODE").build();
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

    private int graphMaxHop(RoutingContext context, RoutingFeatures features, RetrievalProgress progress,
                            boolean replanExhausted) {
        int ceiling = context.getMaxHop();
        if (replanExhausted) {
            // A spent replan budget may still stop or shrink expansion, but it must not fund extra
            // hops. Otherwise a late anchor measurement could grow the plan after the bound that
            // keeps replanning predictable had already been reached.
            return Math.min(1, ceiling);
        }
        boolean strong = features.getAnchorCount() >= minimumAnchorCount
            && features.getAnchorConfidence() >= strongAnchorConfidence
            && features.getCandidateDegree() <= highCandidateDegree;
        // A second hop is only funded when the first one looked healthy. Growth and yield are
        // measured by that first hop, so this is the only point where a two-hop decision can be
        // based on real expansion cost rather than on pre-execution estimates.
        if (!strong || features.getGraphGrowthRate() > highGrowthRate || yieldBelowThreshold(progress)) {
            return Math.min(1, ceiling);
        }
        return Math.min(2, ceiling);
    }

    private int neighborCap(int candidateBudget, int candidateDegree) {
        if (candidateDegree > highCandidateDegree) {
            return Math.max(1, Math.min(baseNeighborCap, candidateBudget / 2));
        }
        return Math.max(1, Math.min(baseNeighborCap, candidateBudget));
    }

    /**
     * Sets the fusion weights when the text balance is decoupled from the anchor decision.
     *
     * <p>Lexical and semantic features choose between the two text channels; the anchor only
     * decides whether graph expansion is funded. With the coupled structure the anchored branch
     * won first, so those rules had no decision domain at all: they fired on 1 of 200 measured
     * queries. The graph weight is the configured one in every branch, so no branch can
     * accidentally multiply it.
     */
    private void applyDecoupledFusion(RetrievalPlan.Builder builder, RoutingFeatures features,
                                      boolean anchored) {
        if (features.getAverageIdf() >= highAverageIdf) {
            builder.bm25Weight(lexicalBm25Weight).vectorWeight(lexicalVectorWeight)
                .reason("LEXICAL_CONFIDENCE");
        } else if (features.getQueryLength() >= longQueryLength) {
            builder.bm25Weight(semanticBm25Weight).vectorWeight(semanticVectorWeight)
                .reason("SEMANTIC_QUERY");
        } else {
            builder.bm25Weight(textWeight).vectorWeight(textWeight);
        }
        builder.graphWeight(graphWeight)
            .reason(anchored ? "GRAPH_ANCHORED_EXPANSION" : "GRAPH_BOUNDED_ANCHOR_FALLBACK");
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
