/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 */
package org.apache.geaflow.ai.retrieval.answer;

import java.util.List;

/**
 * Citation carrying the full provenance the design requires for every returned evidence item.
 *
 * <p>The design's definition of done states that "every graph hit is traceable to query, mode, seed,
 * relation path, evidence, and source document". The original four-field citation could not satisfy
 * that: it dropped the query identity, the selected mode, the anchor seed and the source document,
 * so an answer could be quoted but never audited back to the search that produced it.
 *
 * <p>Fields are immutable. The legacy four-argument constructor is retained so existing callers keep
 * compiling; it leaves the new provenance fields unset, and {@link #hasFullProvenance()} reports
 * that explicitly instead of pretending the citation is complete.
 */
public final class Citation {
    private final String evidenceId;
    private final String sourceId;
    private final String chunkId;
    private final String pathId;
    private final String queryId;
    private final String mode;
    private final String seedEntityId;
    private final String documentId;
    private final String sourceUri;
    private final List<String> relationPath;
    private final int hop;

    public Citation(String evidenceId, String sourceId) {
        this(evidenceId, sourceId, null);
    }

    public Citation(String evidenceId, String sourceId, String chunkId) {
        this(evidenceId, sourceId, chunkId, null);
    }

    public Citation(String evidenceId, String sourceId, String chunkId, String pathId) {
        this(evidenceId, sourceId, chunkId, pathId, null, null, null, null, null,
            java.util.Collections.emptyList(), 0);
    }

    /**
     * Creates a citation with the complete provenance bundle required by the design.
     *
     * <p>Bundles query identity, selected mode, anchor seed, source document, source URI, the
     * ordered relation path and its hop count.
     *
     * @param queryId deterministic query identity the evidence was retrieved for
     * @param mode selected retrieval mode that produced the evidence
     * @param seedEntityId anchor entity the graph expansion started from
     * @param documentId source document that ultimately contains the cited text
     * @param sourceUri resolvable source location for the cited document
     * @param relationPath ordered relation labels traversed to reach the evidence
     * @param hop number of graph hops traversed; zero for non-graph evidence
     */
    public Citation(String evidenceId, String sourceId, String chunkId, String pathId,
                    String queryId, String mode, String seedEntityId, String documentId,
                    String sourceUri, List<String> relationPath, int hop) {
        if (evidenceId == null || evidenceId.trim().isEmpty() || sourceId == null
            || sourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("citation evidenceId and sourceId are required");
        }
        if (hop < 0) {
            throw new IllegalArgumentException("citation hop must not be negative");
        }
        this.evidenceId = evidenceId;
        this.sourceId = sourceId;
        this.chunkId = chunkId;
        this.pathId = pathId;
        this.queryId = queryId;
        this.mode = mode;
        this.seedEntityId = seedEntityId;
        this.documentId = documentId;
        this.sourceUri = sourceUri;
        this.relationPath = relationPath == null
            ? java.util.Collections.emptyList()
            : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(relationPath));
        this.hop = hop;
    }

    /**
     * Returns a copy carrying the extra provenance discovered while assembling the answer.
     *
     * @param provenance source of the query, mode, seed, document and relation path
     */
    public Citation withProvenance(CitationProvenance provenance) {
        if (provenance == null) {
            return this;
        }
        return new Citation(evidenceId, sourceId, chunkId, pathId, provenance.getQueryId(),
            provenance.getMode(), provenance.getSeedEntityId(),
            provenance.getDocumentId() == null ? documentId : provenance.getDocumentId(),
            provenance.getSourceUri() == null ? sourceUri : provenance.getSourceUri(),
            provenance.getRelationPath(), provenance.getHop());
    }

    public String getEvidenceId() {
        return evidenceId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getChunkId() {
        return chunkId;
    }

    public String getPathId() {
        return pathId;
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

    /**
     * Returns true when the citation identifies the query, the mode, the source document and, for a
     * graph hit, the anchor seed and relation path. Legacy citations return false.
     */
    public boolean hasFullProvenance() {
        if (queryId == null || mode == null || documentId == null || sourceUri == null) {
            return false;
        }
        return hop == 0 || (seedEntityId != null && !relationPath.isEmpty());
    }
}