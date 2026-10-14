/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Offline extractive consumer that never emits unsupported text. */
public final class ExtractiveAnswerConsumer {
    private final TokenEstimator estimator;

    public ExtractiveAnswerConsumer(TokenEstimator estimator) {
        this.estimator = java.util.Objects.requireNonNull(estimator, "estimator");
    }

    public AnswerResponse consume(AnswerRequest request, List<Evidence> evidence) {
        if (evidence == null || evidence.isEmpty()) {
            return abstain("NO_EVIDENCE");
        }
        int remaining = request.getTokenBudget();
        StringBuilder context = new StringBuilder();
        List<Citation> citations = new ArrayList<>();
        Set<String> sources = new HashSet<>();
        for (Evidence item : evidence) {
            if (item == null || item.getEvidenceId() == null || item.getText() == null
                || item.getSources().isEmpty()) {
                continue;
            }
            int cost = item.getChunks().isEmpty() ? Math.max(1, item.getText().length() / 4)
                : estimator.estimate(item.getChunks().get(0));
            if (cost > remaining) {
                continue;
            }
            SourceRef source = item.getSources().get(0);
            if (sources.add(source.getDocumentId())) {
                context.append(item.getText()).append('\n');
                citations.add(new Citation(item.getEvidenceId(), source.getDocumentId()));
                remaining -= cost;
            }
        }
        if (citations.isEmpty()) {
            return abstain("TOKEN_BUDGET_OR_INVALID_PROVENANCE");
        }
        String answer = citations.size() == 1 ? context.toString().trim() : context.toString().trim();
        return new AnswerResponse(answer, null, context.toString().trim(),
            request.getTokenBudget() - remaining, citations);
    }

    private static AnswerResponse abstain(String reason) {
        return new AnswerResponse(null, reason, "", 0, java.util.Collections.emptyList());
    }
}
