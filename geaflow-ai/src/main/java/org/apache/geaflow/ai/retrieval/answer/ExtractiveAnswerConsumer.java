/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Offline extractive consumer that never emits unsupported text. */
public final class ExtractiveAnswerConsumer implements AnswerSynthesizer {
    private final TokenEstimator estimator;

    public ExtractiveAnswerConsumer(TokenEstimator estimator) {
        this.estimator = java.util.Objects.requireNonNull(estimator, "estimator");
    }

    @Override
    public AnswerResponse synthesize(AnswerRequest request, List<Evidence> evidence) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (evidence == null || evidence.isEmpty()) {
            return abstain("NO_EVIDENCE", request);
        }
        int remaining = request.getTokenBudget();
        Map<String, String> suppressReasons = new LinkedHashMap<>();
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
            if (item == null) {
                continue;
            }
            if (item.getEvidenceId() == null || item.getText() == null
                || (item.getSources().isEmpty() && item.getPaths().isEmpty())) {
                if (item.getEvidenceId() != null) {
                    suppressReasons.put(item.getEvidenceId(), "INVALID_PROVENANCE");
                }
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
                    suppressReasons.put(item.getEvidenceId(), "TOKEN_BUDGET_EXCEEDED");
                    continue;
                }
                if (sources.contains(identity)) {
                    suppressReasons.put(item.getEvidenceId(), "DUPLICATE_CHUNK");
                    continue;
                }
                if (sources.add(identity)) {
                    context.append(item.getText()).append('\n');
                    evidenceIds.add(item.getEvidenceId());
                    item.getGraphTraces().forEach(trace -> traceIds.add(trace.getQueryId()));
                    if (!item.getPaths().isEmpty()) {
                        item.getPaths().forEach(path -> citations.add(
                            new Citation(item.getEvidenceId(), citationSourceId, null,
                                pathIdentity(path)).withProvenance(
                                    provenance(request, item, source, path))));
                    } else {
                        citations.add(new Citation(item.getEvidenceId(), citationSourceId)
                            .withProvenance(provenance(request, item, source, null)));
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
                        suppressReasons.put(item.getEvidenceId(), "DUPLICATE_CHUNK");
                        continue;
                    }
                    int estimate = estimator.estimate(chunk);
                    if (estimate < 1) {
                        throw new IllegalArgumentException("token estimator must return a positive value");
                    }
                    if (selectedCost + estimate > remaining) {
                        localChunks.remove(identity);
                        suppressReasons.put(item.getEvidenceId(), "TOKEN_BUDGET_EXCEEDED");
                        continue;
                    }
                    selectedText.append(chunk.getText()).append('\n');
                    selectedCitations.add(new Citation(item.getEvidenceId(), citationSourceId,
                        chunk.getChunkId()).withProvenance(
                            provenance(request, item, source, primaryPath(item))));
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
            return abstain("TOKEN_BUDGET_OR_INVALID_PROVENANCE", request, suppressReasons,
                ordered.size());
        }
        String answer = context.toString().trim();
        if (answer.isEmpty()) {
            return abstain("EMPTY_SUPPORTED_ANSWER", request, suppressReasons, ordered.size());
        }
        return new AnswerResponse(answer, null, context.toString().trim(),
            request.getTokenBudget() - remaining, citations, evidenceIds,
            new ArrayList<>(traceIds), ordered.size(), suppressReasons, request.getRequestId(),
            request.getGraphVersion(), request.getIndexVersion());
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

    private static org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef primaryPath(
        Evidence item) {
        return item.getPaths().isEmpty() ? null : item.getPaths().get(0);
    }

    /**
     * Builds the provenance bundle for one citation.
     *
     * <p>Gathers query identity, selected mode, anchor seed, source document, source URI and the
     * ordered relation path so the design's traceability requirement holds for every citation, not
     * only for graph hits. Text-only evidence still records query, mode and source document.
     */
    private static CitationProvenance provenance(AnswerRequest request, Evidence item,
                                                 SourceRef source,
                                                 org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef pathRef) {
        org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace trace = null;
        if (pathRef != null) {
            for (org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace candidate
                    : item.getGraphTraces()) {
                if (candidate.getPath().sameIdentityAs(pathRef)) {
                    trace = candidate;
                    break;
                }
            }
        } else if (item.getGraphTraces().size() == 1) {
            trace = item.getGraphTraces().get(0);
        }
        String queryId = null;
        String mode = request.getMode();
        String seedEntityId = null;
        if (trace != null) {
            queryId = trace.getQueryId();
            if (trace.getSelectedMode() != null) {
                mode = trace.getSelectedMode().name();
            }
            if (trace.getSeed() != null) {
                seedEntityId = trace.getSeed().getEntityId();
            }
        }
        if (queryId == null) {
            queryId = request.getRequestId();
        }
        List<String> relationPath = new ArrayList<>();
        int hop = 0;
        if (pathRef != null) {
            hop = pathRef.getHop();
        }
        if (trace != null) {
            for (org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef edge
                    : trace.getPath().getEdges()) {
                relationPath.add(edge.getLabel());
            }
            if (trace.getPath() != null) {
                hop = Math.max(hop, trace.getPath().getHop());
            }
        }
        if (relationPath.isEmpty() && pathRef != null) {
            relationPath.addAll(pathRef.getEdgeIds());
        }
        String documentId = source == null ? null : source.getDocumentId();
        String sourceUri = source == null ? null : source.getSourceUri();
        if (documentId == null && !item.getChunks().isEmpty()) {
            documentId = item.getChunks().get(0).getDocumentId();
            sourceUri = item.getChunks().get(0).getSourceUri();
        }
        return new CitationProvenance(queryId, mode, seedEntityId, documentId, sourceUri,
            relationPath, hop);
    }

    private static AnswerResponse abstain(String reason, AnswerRequest request) {
        return abstain(reason, request, java.util.Collections.emptyMap(), 0);
    }

    private static AnswerResponse abstain(String reason, AnswerRequest request,
                                          Map<String, String> suppressReasons,
                                          int consideredEvidenceCount) {
        return new AnswerResponse(null, reason, "", 0, java.util.Collections.emptyList(),
            java.util.Collections.emptyList(), java.util.Collections.emptyList(),
            consideredEvidenceCount, suppressReasons, request.getRequestId(),
            request.getGraphVersion(), request.getIndexVersion());
    }
}
