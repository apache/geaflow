/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Structured answer result; abstention is explicit and citation-bearing. */
public final class AnswerResponse {
    private final String answer;
    private final String abstentionReason;
    private final String context;
    private final int tokens;
    private final List<Citation> citations;
    private final List<String> evidenceIds;
    private final List<String> traceIds;
    private final int consideredEvidenceCount;
    private final Map<String, String> suppressReasons;
    private final String requestId;
    private final String graphVersion;
    private final String indexVersion;

    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations) {
        this(answer, abstentionReason, context, tokens, citations,
            Collections.emptyList(), Collections.emptyList());
    }

    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations, List<String> evidenceIds, List<String> traceIds) {
        this(answer, abstentionReason, context, tokens, citations, evidenceIds, traceIds,
            evidenceIds.size());
    }

    /**
     * Builds a response that also records how much evidence the consumer was offered.
     *
     * @param consideredEvidenceCount evidence offered before token truncation, used to report how
     *     much of it actually reached the citation list
     */
    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations, List<String> evidenceIds, List<String> traceIds,
                          int consideredEvidenceCount) {
        this(answer, abstentionReason, context, tokens, citations, evidenceIds, traceIds,
            consideredEvidenceCount, Collections.emptyMap(), null, null, null);
    }

    /**
     * Full result constructor.
     *
     * @param suppressReasons why each offered evidence item was withheld, keyed by evidence id, so
     *     an operator can explain a thin or abstained answer without re-running the search
     * @param requestId retrieval request that produced the evidence
     * @param graphVersion graph version behind the evidence
     * @param indexVersion index version behind the evidence
     */
    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations, List<String> evidenceIds, List<String> traceIds,
                          int consideredEvidenceCount, Map<String, String> suppressReasons,
                          String requestId, String graphVersion, String indexVersion) {
        if (tokens < 0) {
            throw new IllegalArgumentException("tokens must not be negative");
        }
        if (consideredEvidenceCount < 0) {
            throw new IllegalArgumentException("consideredEvidenceCount must not be negative");
        }
        if (citations == null) {
            throw new IllegalArgumentException("citations are required");
        }
        if (evidenceIds == null || traceIds == null) {
            throw new IllegalArgumentException("evidenceIds and traceIds are required");
        }
        if (abstentionReason != null && answer != null) {
            throw new IllegalArgumentException("abstained response must not contain an answer");
        }
        if (abstentionReason == null && (answer == null || answer.trim().isEmpty()
            || citations.isEmpty())) {
            throw new IllegalArgumentException("successful response requires answer and citations");
        }
        this.answer = answer;
        this.abstentionReason = abstentionReason;
        this.context = context == null ? "" : context;
        this.tokens = tokens;
        this.citations = Collections.unmodifiableList(new ArrayList<>(citations));
        this.evidenceIds = Collections.unmodifiableList(new ArrayList<>(evidenceIds));
        this.traceIds = Collections.unmodifiableList(new ArrayList<>(traceIds));
        this.consideredEvidenceCount = consideredEvidenceCount;
        this.suppressReasons = suppressReasons == null ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(suppressReasons));
        this.requestId = requestId;
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
    }

    public String getAnswer() {
        return answer;
    }

    public String getAbstentionReason() {
        return abstentionReason;
    }

    public String getContext() {
        return context;
    }

    public int getTokens() {
        return tokens;
    }

    public List<Citation> getCitations() {
        return citations;
    }

    public List<String> getEvidenceIds() {
        return evidenceIds;
    }

    public List<String> getTraceIds() {
        return traceIds;
    }

    /**
     * Fraction of the offered evidence that is actually cited, in {@code [0, 1]}.
     *
     * <p>Zero when no evidence was offered, so an abstained response reports 0.0 instead of NaN.
     */
    public double getCitationCoverage() {
        if (consideredEvidenceCount == 0) {
            return 0.0D;
        }
        return (double) evidenceIds.size() / (double) consideredEvidenceCount;
    }

    public int getConsideredEvidenceCount() {
        return consideredEvidenceCount;
    }

    public boolean isAbstained() {
        return abstentionReason != null;
    }

    /** Returns why each offered evidence item was withheld, keyed by evidence id. */
    public Map<String, String> getSuppressReasons() {
        return suppressReasons;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }
}
