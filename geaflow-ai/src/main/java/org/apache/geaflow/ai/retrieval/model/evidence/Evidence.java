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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
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
    @SerializedName("paths")
    private final List<GraphPathRef> paths;
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

    private Evidence() {
        this.evidenceId = null;
        this.kind = null;
        this.text = null;
        this.chunks = Collections.emptyList();
        this.entities = Collections.emptyList();
        this.paths = Collections.emptyList();
        this.graphTraces = Collections.emptyList();
        this.sources = Collections.emptyList();
        this.stageScores = Collections.emptyMap();
        this.fusedScore = null;
        this.finalScore = null;
        this.rank = null;
    }

    public Evidence(String evidenceId, EvidenceKind kind, String text, List<TextChunk> chunks,
                    List<EntityRef> entities, List<GraphPathRef> paths, List<SourceRef> sources,
                    Map<String, ChannelScore> stageScores, Double fusedScore, Integer rank) {
        this(evidenceId, kind, text, chunks, entities, paths, sources, stageScores, fusedScore,
            null, rank, Collections.emptyList());
    }

    public Evidence(String evidenceId, EvidenceKind kind, String text, List<TextChunk> chunks,
                    List<EntityRef> entities, List<GraphPathRef> paths, List<SourceRef> sources,
                    Map<String, ChannelScore> stageScores, Double fusedScore, Double finalScore,
                    Integer rank) {
        this(evidenceId, kind, text, chunks, entities, paths, sources, stageScores, fusedScore,
            finalScore, rank, Collections.emptyList());
    }

    public Evidence(String evidenceId, EvidenceKind kind, String text, List<TextChunk> chunks,
                    List<EntityRef> entities, List<GraphPathRef> paths, List<SourceRef> sources,
                    Map<String, ChannelScore> stageScores, Double fusedScore, Double finalScore,
                    Integer rank, List<GraphEvidenceTrace> graphTraces) {
        this.evidenceId = ModelValidation.optionalNonBlank(evidenceId, "evidenceId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.text = ModelValidation.optional(text);
        this.chunks = ModelValidation.immutableList(chunks, "chunks");
        this.entities = ModelValidation.immutableList(entities, "entities");
        this.paths = ModelValidation.immutableList(paths, "paths");
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
        return Collections.unmodifiableList(paths == null ? Collections.emptyList() : paths);
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
            && Objects.equals(entities, that.entities) && Objects.equals(paths, that.paths)
            && Objects.equals(sources, that.sources) && Objects.equals(graphTraces, that.graphTraces)
            && Objects.equals(stageScores, that.stageScores)
            && Objects.equals(fusedScore, that.fusedScore)
            && Objects.equals(finalScore, that.finalScore)
            && Objects.equals(rank, that.rank);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evidenceId, kind, text, chunks, entities, paths, sources, graphTraces,
            stageScores, fusedScore, finalScore, rank);
    }
}
