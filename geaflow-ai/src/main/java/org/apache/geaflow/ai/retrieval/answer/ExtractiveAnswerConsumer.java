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
        List<String> evidenceIds = new ArrayList<>();
        Set<String> traceIds = new java.util.TreeSet<>();
        Set<String> sources = new HashSet<>();
        List<Evidence> ordered = new ArrayList<>(evidence);
        ordered.removeIf(java.util.Objects::isNull);
        ordered.sort(Comparator.comparingDouble(ExtractiveAnswerConsumer::score)
            .reversed().thenComparing(Evidence::getEvidenceId,
                Comparator.nullsFirst(String::compareTo)));
        for (Evidence item : ordered) {
            if (item == null || item.getEvidenceId() == null || item.getText() == null
                || (item.getSources().isEmpty() && item.getPaths().isEmpty())) {
                continue;
            }
            SourceRef source = item.getSources().isEmpty() ? null : item.getSources().get(0);
            String sourceId = source == null ? item.getGraphVersion() : source.getDocumentId();
            if (sourceId == null && !item.getPaths().isEmpty()) {
                sourceId = "graph";
            }
            final String citationSourceId = sourceId;
            if (item.getChunks().isEmpty()) {
                String identity = citationSourceId + "\u0000" + item.getEvidenceId();
                int cost = Math.max(1, (item.getText().length() + 3) / 4);
                if (cost > remaining) {
                    continue;
                }
                if (sources.add(identity)) {
                    context.append(item.getText()).append('\n');
                    evidenceIds.add(item.getEvidenceId());
                    item.getGraphTraces().forEach(trace -> traceIds.add(trace.getQueryId()));
                    if (!item.getPaths().isEmpty()) {
                        item.getPaths().forEach(path -> citations.add(new Citation(item.getEvidenceId(),
                            citationSourceId, null, pathIdentity(path))));
                    } else {
                        citations.add(new Citation(item.getEvidenceId(), citationSourceId));
                    }
                    remaining -= cost;
                }
            } else {
                StringBuilder selectedText = new StringBuilder();
                List<Citation> selectedCitations = new ArrayList<>();
                Set<String> localChunks = new HashSet<>();
                long selectedCost = 0L;
                for (org.apache.geaflow.ai.retrieval.model.document.TextChunk chunk : item.getChunks()) {
                    String identity = citationSourceId + "\u0000" + chunk.getChunkId();
                    if (sources.contains(identity) || !localChunks.add(identity)) {
                        continue;
                    }
                    int estimate = estimator.estimate(chunk);
                    if (estimate < 1) {
                        throw new IllegalArgumentException("token estimator must return a positive value");
                    }
                    if (selectedCost + estimate > remaining) {
                        localChunks.remove(identity);
                        continue;
                    }
                    selectedText.append(chunk.getText()).append('\n');
                    selectedCitations.add(new Citation(item.getEvidenceId(), citationSourceId,
                        chunk.getChunkId()));
                    selectedCost = Math.addExact(selectedCost, estimate);
                }
                if (selectedCitations.isEmpty()) {
                    continue;
                }
                context.append(selectedText);
                citations.addAll(selectedCitations);
                evidenceIds.add(item.getEvidenceId());
                item.getGraphTraces().forEach(trace -> traceIds.add(trace.getQueryId()));
                sources.addAll(localChunks);
                remaining -= (int) selectedCost;
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
            request.getTokenBudget() - remaining, citations, evidenceIds,
            new ArrayList<>(traceIds), ordered.size());
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

    private static String pathIdentity(org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef path) {
        return "hop=" + path.getHop() + ";vertices=" + String.join(",", path.getVertexIds())
            + ";edges=" + String.join(",", path.getEdgeIds());
    }

    private static AnswerResponse abstain(String reason) {
        return new AnswerResponse(null, reason, "", 0, java.util.Collections.emptyList());
    }
}
