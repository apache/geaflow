/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Structured answer result; abstention is explicit and citation-bearing. */
public final class AnswerResponse {
    private final String answer;
    private final String abstentionReason;
    private final String context;
    private final int tokens;
    private final List<Citation> citations;
    private final List<String> evidenceIds;
    private final List<String> traceIds;

    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations) {
        this(answer, abstentionReason, context, tokens, citations,
            Collections.emptyList(), Collections.emptyList());
    }

    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations, List<String> evidenceIds, List<String> traceIds) {
        if (tokens < 0) {
            throw new IllegalArgumentException("tokens must not be negative");
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

    public boolean isAbstained() {
        return abstentionReason != null;
    }
}
