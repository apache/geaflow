/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

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
        this.answer = answer;
        this.abstentionReason = abstentionReason;
        this.context = context == null ? "" : context;
        this.tokens = tokens;
        this.citations = Collections.unmodifiableList(new java.util.ArrayList<>(citations));
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
