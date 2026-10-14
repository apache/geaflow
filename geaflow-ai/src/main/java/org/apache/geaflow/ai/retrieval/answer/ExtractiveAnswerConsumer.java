/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.ArrayList;
import java.util.Comparator;
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
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (evidence == null || evidence.isEmpty()) {
            return abstain("NO_EVIDENCE");
        }
        int remaining = request.getTokenBudget();
        StringBuilder context = new StringBuilder();
        List<Citation> citations = new ArrayList<>();
        Set<String> sources = new HashSet<>();
        List<Evidence> ordered = new ArrayList<>(evidence);
        ordered.removeIf(java.util.Objects::isNull);
        ordered.sort(Comparator.comparingDouble(ExtractiveAnswerConsumer::score)
            .reversed().thenComparing(Evidence::getEvidenceId,
                Comparator.nullsFirst(String::compareTo)));
        for (Evidence item : ordered) {
            if (item == null || item.getEvidenceId() == null || item.getText() == null
                || item.getSources().isEmpty()) {
                continue;
            }
            long estimatedCost = item.getChunks().isEmpty() ? Math.max(1L, (item.getText().length() + 3L) / 4L)
                : item.getChunks().stream().mapToLong(chunk -> {
                    int estimate = estimator.estimate(chunk);
                    if (estimate < 1) {
                        throw new IllegalArgumentException("token estimator must return a positive value");
                    }
                    return estimate;
                }).sum();
            if (estimatedCost > remaining || estimatedCost > Integer.MAX_VALUE) {
                continue;
            }
            int cost = (int) estimatedCost;
            SourceRef source = item.getSources().get(0);
            String chunkId = item.getChunks().isEmpty() ? null : item.getChunks().get(0).getChunkId();
            String identity = source.getDocumentId() + "\u0000" + String.valueOf(chunkId);
            if (sources.add(identity)) {
                context.append(item.getText()).append('\n');
                citations.add(new Citation(item.getEvidenceId(), source.getDocumentId(), chunkId));
                remaining -= cost;
            }
        }
        if (citations.isEmpty()) {
            return abstain("TOKEN_BUDGET_OR_INVALID_PROVENANCE");
        }
        String answer = context.toString().trim();
        if (answer.isEmpty()) {
            return abstain("EMPTY_SUPPORTED_ANSWER");
        }
        return new AnswerResponse(answer, null, context.toString().trim(),
            request.getTokenBudget() - remaining, citations);
    }

    private static double score(Evidence evidence) {
        if (evidence.getFinalScore() != null) {
            return evidence.getFinalScore();
        }
        if (evidence.getFusedScore() != null) {
            return evidence.getFusedScore();
        }
        return evidence.getRank() == null ? 0.0D : -evidence.getRank();
    }

    private static AnswerResponse abstain(String reason) {
        return new AnswerResponse(null, reason, "", 0, java.util.Collections.emptyList());
    }
}
