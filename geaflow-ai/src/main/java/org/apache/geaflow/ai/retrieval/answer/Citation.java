/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package org.apache.geaflow.ai.retrieval.answer;

/** Citation bound to one returned evidence identifier and source identifier. */
public final class Citation {
    private final String evidenceId;
    private final String sourceId;

    public Citation(String evidenceId, String sourceId) {
        if (evidenceId == null || evidenceId.trim().isEmpty() || sourceId == null
            || sourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("citation evidenceId and sourceId are required");
        }
        this.evidenceId = evidenceId;
        this.sourceId = sourceId;
    }

    public String getEvidenceId() {
        return evidenceId;
    }

    public String getSourceId() {
        return sourceId;
    }
}
