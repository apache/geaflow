/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.Collections;
import java.util.List;

/**
 * Immutable provenance bundle attached to every citation produced by the answer consumer.
 *
 * <p>It bundles exactly the fields the design requires for a traceable graph hit: the query
 * identity, the selected mode, the anchor seed, the source document and source URI, and the ordered
 * relation path with its hop count.
 */
public final class CitationProvenance {
    private final String queryId;
    private final String mode;
    private final String seedEntityId;
    private final String documentId;
    private final String sourceUri;
    private final List<String> relationPath;
    private final int hop;

    /**
     * Creates a provenance bundle for one citation.
     *
     * @param queryId deterministic query identity the evidence was retrieved for
     * @param mode selected retrieval mode that produced the evidence
     * @param seedEntityId anchor entity the graph expansion started from, or null for text hits
     * @param documentId source document containing the cited text
     * @param sourceUri resolvable source location for the cited document
     * @param relationPath ordered relation labels traversed to reach the evidence
     * @param hop number of graph hops traversed; zero for non-graph evidence
     */
    public CitationProvenance(String queryId, String mode, String seedEntityId, String documentId,
                              String sourceUri, List<String> relationPath, int hop) {
        if (hop < 0) {
            throw new IllegalArgumentException("hop must not be negative");
        }
        this.queryId = queryId;
        this.mode = mode;
        this.seedEntityId = seedEntityId;
        this.documentId = documentId;
        this.sourceUri = sourceUri;
        this.relationPath = relationPath == null ? Collections.emptyList()
            : Collections.unmodifiableList(new java.util.ArrayList<>(relationPath));
        this.hop = hop;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getMode() {
        return mode;
    }

    public String getSeedEntityId() {
        return seedEntityId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public List<String> getRelationPath() {
        return relationPath;
    }

    public int getHop() {
        return hop;
    }
}