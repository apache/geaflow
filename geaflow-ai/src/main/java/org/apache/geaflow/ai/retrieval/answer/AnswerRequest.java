/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

/** Request for deterministic extractive answer consumption. */
public final class AnswerRequest {
    private final String question;
    private final int tokenBudget;

    public AnswerRequest(String question, int tokenBudget) {
        if (question == null || question.trim().isEmpty() || tokenBudget < 1) {
            throw new IllegalArgumentException("question and positive tokenBudget are required");
        }
        this.question = question;
        this.tokenBudget = tokenBudget;
    }

    public String getQuestion() {
        return question;
    }

    public int getTokenBudget() {
        return tokenBudget;
    }
}
