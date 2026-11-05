/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.retrieval.model.evidence;

import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;

/**
 * Immutable retrieval evidence with optional payload references and multi-channel scores.
 *
 * <p>Complete value equality includes presentation, provenance, and ranking fields. Identity
 * comparison uses the evidence kind and nested reference identities, allowing candidates from
 * different retrieval channels to be merged without losing their scores. When no references are
 * available, evidenceId or text provides the fallback identity.</p>
 */
public final class Evidence {

    public static final int CURRENT_SCHEMA_VERSION = 2;

    @SerializedName("evidenceId")
    private final String evidenceId;
    @SerializedName("kind")
    private final EvidenceKind kind;
    @SerializedName("text")
    private final String text;
    @SerializedName("chunks")
    private final List<TextChunk> chunks;
    @SerializedName("entities")
    private final List<EntityRef> entities;
    @SerializedName("graphTraces")
    private final List<GraphEvidenceTrace> graphTraces;
    @SerializedName("sources")
    private final List<SourceRef> sources;
    @SerializedName("stageScores")
    private final Map<String, ChannelScore> stageScores;
    @SerializedName("fusedScore")
    private final Double fusedScore;
    @SerializedName("finalScore")
    private final Double finalScore;
    @SerializedName("rank")
    private final Integer rank;
    @SerializedName("schemaVersion")
    private final int schemaVersion;
    @SerializedName("dataset")
    private final String dataset;
    @SerializedName("datasetVersion")
    private final String datasetVersion;
    @SerializedName("split")
    private final String split;
    @SerializedName("graphVersion")
    private final String graphVersion;
    @SerializedName("indexVersion")
    private final String indexVersion;

    private Evidence() {
        this.evidenceId = null;
        this.kind = null;
        this.text = null;
        this.chunks = Collections.emptyList();
        this.entities = Collections.emptyList();
        this.graphTraces = Collections.emptyList();
        this.sources = Collections.emptyList();
        this.stageScores = Collections.emptyMap();
        this.fusedScore = null;
        this.finalScore = null;
        this.rank = null;
        this.schemaVersion = CURRENT_SCHEMA_VERSION;
        this.dataset = null;
        this.datasetVersion = null;
        this.split = null;
        this.graphVersion = null;
        this.indexVersion = null;
    }

    public Evidence(String evidenceId, EvidenceKind kind, String text, List<TextChunk> chunks,
                    List<EntityRef> entities, List<SourceRef> sources,
                    Map<String, ChannelScore> stageScores, Double fusedScore, Double finalScore,
                    Integer rank, List<GraphEvidenceTrace> graphTraces, int schemaVersion,
                    String dataset, String datasetVersion, String split, String graphVersion,
                    String indexVersion) {
        this.evidenceId = ModelValidation.optionalNonBlank(evidenceId, "evidenceId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.text = ModelValidation.optional(text);
        this.chunks = ModelValidation.immutableList(chunks, "chunks");
        this.entities = ModelValidation.immutableList(entities, "entities");
        this.graphTraces = ModelValidation.immutableList(graphTraces, "graphTraces");
        this.sources = ModelValidation.immutableList(sources, "sources");
        this.stageScores = ModelValidation.sortedMap(stageScores);
        for (Map.Entry<String, ChannelScore> entry : this.stageScores.entrySet()) {
            if (entry.getValue() == null) {
                throw new NullPointerException("stage score must not be null");
            }
            if (!entry.getKey().equals(entry.getValue().getChannel())) {
                throw new RetrievalModelValidationException("stage score key must match channel");
            }
        }
        this.fusedScore = ModelValidation.optionalScore(fusedScore, "fusedScore");
        this.finalScore = finalScore == null ? null : ModelValidation.finite(finalScore, "finalScore");
        this.rank = ModelValidation.optionalRank(rank, "rank");
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new RetrievalModelValidationException("unsupported evidence schemaVersion: "
                + schemaVersion);
        }
        this.schemaVersion = schemaVersion;
        this.dataset = ModelValidation.optionalNonBlank(dataset, "dataset");
        this.datasetVersion = ModelValidation.optionalNonBlank(datasetVersion, "datasetVersion");
        this.split = ModelValidation.optionalNonBlank(split, "split");
        this.graphVersion = ModelValidation.optionalNonBlank(graphVersion, "graphVersion");
        this.indexVersion = ModelValidation.optionalNonBlank(indexVersion, "indexVersion");
        validateProvenance();
    }

    public String getEvidenceId() {
        return evidenceId;
    }

    public EvidenceKind getKind() {
        return kind;
    }

    public String getText() {
        return text;
    }

    public List<TextChunk> getChunks() {
        return Collections.unmodifiableList(chunks == null ? Collections.emptyList() : chunks);
    }

    public List<EntityRef> getEntities() {
        return Collections.unmodifiableList(entities == null ? Collections.emptyList() : entities);
    }

    /** Returns the primary entity identifier for entity evidence, when present. */
    public String getEntityId() {
        return entities == null || entities.isEmpty() ? null : entities.get(0).getEntityId();
    }

    public List<GraphPathRef> getPaths() {
        List<GraphPathRef> unique = new ArrayList<>();
        for (GraphEvidenceTrace trace : getGraphTraces()) {
            GraphPathRef path = trace.getPath();
            if (unique.stream().noneMatch(existing -> existing.sameIdentityAs(path))) {
                unique.add(path);
            }
        }
        return Collections.unmodifiableList(unique);
    }

    public List<GraphEvidenceTrace> getGraphTraces() {
        return Collections.unmodifiableList(graphTraces == null
            ? Collections.emptyList() : graphTraces);
    }

    public List<SourceRef> getSources() {
        return Collections.unmodifiableList(sources == null ? Collections.emptyList() : sources);
    }

    public Map<String, ChannelScore> getStageScores() {
        return Collections.unmodifiableMap(stageScores == null
            ? Collections.emptyMap() : stageScores);
    }

    public Double getFusedScore() {
        return fusedScore;
    }

    public Double getFinalScore() {
        return finalScore;
    }

    public Integer getRank() {
        return rank;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getDataset() {
        return dataset;
    }

    public String getDatasetVersion() {
        return datasetVersion;
    }

    public String getSplit() {
        return split;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public boolean sameIdentityAs(Evidence other) {
        if (this == other) {
            return true;
        }
        if (other == null || kind != other.kind) {
            return false;
        }
        if (hasReferences() || other.hasReferences()) {
            return sameMultiset(getChunks(), other.getChunks(), TextChunk::sameIdentityAs)
                && sameMultiset(getEntities(), other.getEntities(), EntityRef::sameIdentityAs)
                && sameMultiset(getPaths(), other.getPaths(), GraphPathRef::sameIdentityAs)
                && sameMultiset(getSources(), other.getSources(), SourceRef::sameIdentityAs);
        }
        if (evidenceId != null && other.evidenceId != null) {
            return evidenceId.equals(other.evidenceId);
        }
        return text != null && other.text != null && text.equals(other.text);
    }

    private boolean hasReferences() {
        return !getChunks().isEmpty() || !getEntities().isEmpty()
            || !getPaths().isEmpty() || !getSources().isEmpty();
    }

    private void validateProvenance() {
        if (schemaVersion == CURRENT_SCHEMA_VERSION) {
            validateSourceReferences();
        }
        if (schemaVersion == CURRENT_SCHEMA_VERSION && kind == EvidenceKind.GRAPH_PATH
            && getGraphTraces().isEmpty()) {
            throw new RetrievalModelValidationException(
                "current graph path evidence requires at least one graph trace");
        }
        boolean anyDatasetScope = dataset != null || datasetVersion != null || split != null;
        boolean completeDatasetScope = dataset != null && datasetVersion != null && split != null;
        if (anyDatasetScope && !completeDatasetScope) {
            throw new RetrievalModelValidationException(
                "dataset, datasetVersion, and split must be provided together");
        }
        String resolvedDataset = dataset;
        String resolvedDatasetVersion = datasetVersion;
        String resolvedSplit = split;
        boolean scopedChunk = false;
        java.util.Set<String> chunkIds = new java.util.HashSet<>();
        Map<String, TextChunk> chunksById = new java.util.HashMap<>();
        for (TextChunk chunk : getChunks()) {
            chunkIds.add(chunk.getChunkId());
            chunksById.put(chunk.getChunkId(), chunk);
            if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null
                && chunk.getDataset() == null) {
                throw new RetrievalModelValidationException(
                    "current evidence schema requires chunk source provenance");
            }
            if (chunk.getDataset() == null) {
                continue;
            }
            scopedChunk = true;
            resolvedDataset = requireSame(resolvedDataset, chunk.getDataset(), "dataset");
            resolvedDatasetVersion = requireSame(resolvedDatasetVersion,
                chunk.getDatasetVersion(), "datasetVersion");
            resolvedSplit = requireSame(resolvedSplit, chunk.getSplit(), "split");
            boolean hasSource = getSources().stream().anyMatch(source -> source.getDocumentId()
                .equals(chunk.getDocumentId()) && sourceMatchesChunk(source, chunk));
            if (!hasSource) {
                throw new RetrievalModelValidationException(
                    "chunk must have a matching source provenance reference");
            }
        }
        if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null && !getChunks().isEmpty()
            && getSources().isEmpty()) {
            throw new RetrievalModelValidationException(
                "current evidence schema requires source references");
        }
        if (scopedChunk && (resolvedDataset == null || resolvedDatasetVersion == null
            || resolvedSplit == null)) {
            throw new RetrievalModelValidationException("scoped chunks require evidence dataset scope");
        }
        for (SourceRef source : getSources()) {
            if (source.getDataset() == null) {
                continue;
            }
            resolvedDataset = requireSame(resolvedDataset, source.getDataset(), "dataset");
            resolvedDatasetVersion = requireSame(resolvedDatasetVersion,
                source.getDatasetVersion(), "datasetVersion");
            resolvedSplit = requireSame(resolvedSplit, source.getSplit(), "split");
        }
        if (dataset == null && resolvedDataset != null) {
            throw new RetrievalModelValidationException(
                "evidence dataset scope must be explicit when references are scoped");
        }
        for (EntityRef entity : getEntities()) {
            validateReferenceScope(entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(),
                "entity");
            if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null
                && entity.getDataset() == null) {
                throw new RetrievalModelValidationException(
                    "current evidence requires entity dataset provenance");
            }
            if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null
                && entity.getSourceChunkIds().isEmpty()) {
                throw new RetrievalModelValidationException(
                    "current evidence entity requires source chunks");
            }
            if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null) {
                validateSourceHash(entity.getSourceHash(), entity.getSourceChunkIds(), chunksById,
                    "entity");
            }
        }
        if (graphVersion != null) {
            for (GraphEvidenceTrace trace : getGraphTraces()) {
                if (!graphVersion.equals(trace.getGraphVersion())) {
                    throw new RetrievalModelValidationException(
                        "graph trace version does not match evidence graphVersion");
                }
                validateCurrentTrace(trace);
                if (schemaVersion == CURRENT_SCHEMA_VERSION) {
                    for (String chunkId : trace.getSupportingChunkIds()) {
                        if (!chunkIds.contains(chunkId)) {
                            throw new RetrievalModelValidationException(
                                "graph trace supporting chunk is absent from evidence chunks");
                        }
                    }
                    validateTraceSupportingChunks(trace);
                }
                validateSeedScope(trace, chunksById);
            }
        } else {
            for (GraphEvidenceTrace trace : getGraphTraces()) {
                validateCurrentTrace(trace);
                if (schemaVersion == CURRENT_SCHEMA_VERSION) {
                    for (String chunkId : trace.getSupportingChunkIds()) {
                        if (!chunkIds.contains(chunkId)) {
                            throw new RetrievalModelValidationException(
                                "graph trace supporting chunk is absent from evidence chunks");
                        }
                    }
                    validateTraceSupportingChunks(trace);
                }
                validateSeedScope(trace, chunksById);
            }
        }
    }

    private void validateTraceSupportingChunks(GraphEvidenceTrace trace) {
        java.util.Set<String> pathSourceChunkIds = new java.util.HashSet<>();
        for (GraphEdgeRef edge : trace.getPath().getEdges()) {
            pathSourceChunkIds.addAll(edge.getSourceChunkIds());
        }
        if (pathSourceChunkIds.isEmpty()) {
            for (EntityRef entity : getEntities()) {
                if (trace.getAnchorEntityId().equals(entity.getEntityId())) {
                    pathSourceChunkIds.addAll(entity.getSourceChunkIds());
                }
            }
        }
        if (!pathSourceChunkIds.containsAll(trace.getSupportingChunkIds())) {
            throw new RetrievalModelValidationException(
                "graph trace supporting chunks must back its path or seed entity");
        }
    }

    private void validateCurrentTrace(GraphEvidenceTrace trace) {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            return;
        }
        if (graphVersion == null || !graphVersion.equals(trace.getGraphVersion())) {
            throw new RetrievalModelValidationException(
                "current graph trace requires matching evidence graphVersion");
        }
        EntityRef anchor = getEntities().stream()
            .filter(entity -> trace.getAnchorEntityId().equals(entity.getEntityId()))
            .findFirst()
            .orElseThrow(() -> new RetrievalModelValidationException(
                "current graph trace requires its anchor entity in evidence"));
        if (!sameProvenance(trace.getSeed(), anchor)) {
            throw new RetrievalModelValidationException(
                "graph seed provenance does not match its anchor entity");
        }
        if (trace.getSupportingChunkIds().isEmpty()) {
            throw new RetrievalModelValidationException(
                "current graph trace must reference supporting chunks");
        }
    }

    private void validateSeedScope(GraphEvidenceTrace trace, Map<String, TextChunk> chunksById) {
        if (trace.getSeed() == null) {
            return;
        }
        if (dataset != null && trace.getSeed().getDataset() == null
            && schemaVersion == CURRENT_SCHEMA_VERSION) {
            throw new RetrievalModelValidationException("current graph seed requires dataset provenance");
        }
        if (trace.getSeed().getDataset() != null) {
            validateVertexScope(trace.getSeed());
        }
        for (GraphVertexRef vertex : trace.getPath().getVertices()) {
            if (dataset != null && schemaVersion == CURRENT_SCHEMA_VERSION && vertex.getDataset() == null) {
                throw new RetrievalModelValidationException(
                    "current graph path requires vertex dataset provenance");
            }
            if (vertex.getDataset() != null) {
                validateVertexScope(vertex);
            }
        }
        for (GraphEdgeRef edge : trace.getPath().getEdges()) {
            if (dataset != null && schemaVersion == CURRENT_SCHEMA_VERSION && edge.getDataset() == null) {
                throw new RetrievalModelValidationException(
                    "current graph path requires edge dataset provenance");
            }
            validateReferenceScope(edge.getDataset(), edge.getDatasetVersion(), edge.getSplit(), "edge");
            if (schemaVersion == CURRENT_SCHEMA_VERSION && dataset != null) {
                validateSourceHash(edge.getSourceHash(), edge.getSourceChunkIds(),
                    chunksById, "edge");
            }
        }
    }

    private void validateSourceReferences() {
        for (int first = 0; first < getSources().size(); first++) {
            SourceRef source = getSources().get(first);
            for (int second = first + 1; second < getSources().size(); second++) {
                SourceRef other = getSources().get(second);
                if (source.getDocumentId().equals(other.getDocumentId())
                    && Objects.equals(source.getStartOffset(), other.getStartOffset())
                    && Objects.equals(source.getEndOffset(), other.getEndOffset())
                    && !sameSourceDocument(source, other)) {
                    throw new RetrievalModelValidationException(
                        "conflicting source references for the same document span");
                }
            }
        }
    }

    private static boolean sameSourceDocument(SourceRef first, SourceRef second) {
        return Objects.equals(RetrievalIds.normalizeSourceUri(first.getSourceUri()),
            RetrievalIds.normalizeSourceUri(second.getSourceUri()))
            && Objects.equals(first.getDataset(), second.getDataset())
            && Objects.equals(first.getDatasetVersion(), second.getDatasetVersion())
            && Objects.equals(first.getSplit(), second.getSplit())
            && Objects.equals(first.getSourceHash(), second.getSourceHash());
    }

    private void validateSourceHash(String sourceHash, List<String> sourceChunkIds,
                                    Map<String, TextChunk> chunksById, String referenceType) {
        if (sourceChunkIds.isEmpty() || !chunksById.keySet().containsAll(sourceChunkIds)) {
            return;
        }
        List<String> sourceHashes = new java.util.ArrayList<>();
        for (String chunkId : sourceChunkIds) {
            sourceHashes.add(chunksById.get(chunkId).getSourceHash());
        }
        if (!Objects.equals(sourceHash, RetrievalIds.aggregateSourceHash(sourceHashes))) {
            throw new RetrievalModelValidationException(
                referenceType + " sourceHash does not match referenced chunks");
        }
    }

    private void validateVertexScope(GraphVertexRef vertex) {
        validateReferenceScope(vertex.getDataset(), vertex.getDatasetVersion(), vertex.getSplit(),
            "graph vertex");
    }

    private static boolean sameProvenance(GraphVertexRef vertex, EntityRef entity) {
        return Objects.equals(vertex.getDataset(), entity.getDataset())
            && Objects.equals(vertex.getDatasetVersion(), entity.getDatasetVersion())
            && Objects.equals(vertex.getSplit(), entity.getSplit())
            && Objects.equals(vertex.getSourceHash(), entity.getSourceHash());
    }

    private void validateReferenceScope(String referenceDataset, String referenceVersion,
                                        String referenceSplit, String referenceType) {
        if (referenceDataset != null && (!Objects.equals(dataset, referenceDataset)
            || !Objects.equals(datasetVersion, referenceVersion)
            || !Objects.equals(split, referenceSplit))) {
            throw new RetrievalModelValidationException(referenceType
                + " provenance conflicts with evidence");
        }
    }

    private static String requireSame(String expected, String actual, String field) {
        if (expected != null && !expected.equals(actual)) {
            throw new RetrievalModelValidationException("conflicting evidence " + field);
        }
        return actual;
    }

    private static boolean sourceMatchesChunk(SourceRef source, TextChunk chunk) {
        if (!RetrievalIds.normalizeSourceUri(source.getSourceUri()).equals(
            RetrievalIds.normalizeSourceUri(chunk.getSourceUri()))) {
            return false;
        }
        if (source.getStartOffset() != null && (source.getStartOffset() != chunk.getStartOffset()
            || source.getEndOffset() != chunk.getEndOffset())) {
            return false;
        }
        return Objects.equals(source.getDataset(), chunk.getDataset())
            && Objects.equals(source.getDatasetVersion(), chunk.getDatasetVersion())
            && Objects.equals(source.getSplit(), chunk.getSplit())
            && Objects.equals(source.getSourceHash(), chunk.getSourceHash());
    }

    private static <T> boolean sameMultiset(List<T> first, List<T> second,
                                             BiPredicate<T, T> identity) {
        if (first == null || second == null || first.size() != second.size()) {
            return false;
        }
        boolean[] matched = new boolean[second.size()];
        for (T value : first) {
            boolean found = false;
            for (int i = 0; i < second.size(); i++) {
                if (!matched[i] && identity.test(value, second.get(i))) {
                    matched[i] = true;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Evidence)) {
            return false;
        }
        Evidence that = (Evidence) object;
        return Objects.equals(evidenceId, that.evidenceId) && kind == that.kind
            && Objects.equals(text, that.text) && Objects.equals(chunks, that.chunks)
            && Objects.equals(entities, that.entities)
            && Objects.equals(sources, that.sources) && Objects.equals(graphTraces, that.graphTraces)
            && Objects.equals(stageScores, that.stageScores)
            && Objects.equals(fusedScore, that.fusedScore)
            && Objects.equals(finalScore, that.finalScore)
            && Objects.equals(rank, that.rank)
            && schemaVersion == that.schemaVersion
            && Objects.equals(dataset, that.dataset)
            && Objects.equals(datasetVersion, that.datasetVersion)
            && Objects.equals(split, that.split)
            && Objects.equals(graphVersion, that.graphVersion)
            && Objects.equals(indexVersion, that.indexVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evidenceId, kind, text, chunks, entities, sources, graphTraces,
            stageScores, fusedScore, finalScore, rank, schemaVersion, dataset, datasetVersion, split,
            graphVersion, indexVersion);
    }
}
