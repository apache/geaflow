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

    public AnswerResponse(String answer, String abstentionReason, String context, int tokens,
                          List<Citation> citations) {
        if (tokens < 0) {
            throw new IllegalArgumentException("tokens must not be negative");
        }
        if (citations == null) {
            throw new IllegalArgumentException("citations are required");
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

    public boolean isAbstained() {
        return abstentionReason != null;
    }
}
