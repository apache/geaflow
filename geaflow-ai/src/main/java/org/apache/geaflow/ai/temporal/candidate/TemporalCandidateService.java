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

package org.apache.geaflow.ai.temporal.candidate;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/** Read-only retrieval over one scope and one temporal slice. */
public final class TemporalCandidateService {

    private final TemporalQueryService query = new TemporalQueryService();
    private final RetrievalProfile profile;

    public TemporalCandidateService() {
        this(RetrievalProfile.BASELINE);
    }

    public TemporalCandidateService(RetrievalProfile profile) {
        this.profile = Objects.requireNonNull(profile, "profile");
    }

    /** Same-key versions are complete; topK limits only additional soft candidates. */
    public List<FactCandidate> facts(CanonicalSnapshot snapshot, QueryMode mode, FactKey key,
                                     Instant validAt, Instant systemAt, String text, int topK)
        throws IOException {
        validateRetrieval(text, topK);
        Map<String, FactKey> keys = versionKeys(snapshot);
        List<MemoryFactVersion> visible = visible(snapshot, mode, key, validAt, systemAt, keys);
        final Map<String, MemoryFactVersion> byId = new HashMap<>();
        for (MemoryFactVersion version : visible) {
            byId.put(version.getId(), version);
        }
        Map<String, Map<String, String>> documents = documents(visible);
        if (profile.isCandidateStrategy()) {
            return strategyFacts(documents, visible, byId, keys, key, text, topK);
        }
        List<Bm25Index.Hit> hits;
        try (Bm25Index index = new Bm25Index(documents, profile)) {
            hits = index.search(text, Math.max(1, documents.size()));
        }
        Map<String, Double> rankingScores = new HashMap<>();
        Map<String, Bm25Index.Hit> hitsById = new HashMap<>();
        for (Bm25Index.Hit hit : hits) {
            rankingScores.put(hit.getId(), hit.getScore());
            hitsById.put(hit.getId(), hit);
        }
        visible.sort(Comparator.comparingDouble((MemoryFactVersion version) ->
            rankingScores.getOrDefault(version.getId(), 0.0)).reversed()
            .thenComparing(MemoryFactVersion::getId));
        List<FactCandidate> result = new ArrayList<>();
        for (MemoryFactVersion version : visible) {
            if (key.equals(keys.get(version.getId()))) {
                Map<String, Double> scores = new TreeMap<>();
                scores.put("RULE", 1.0);
                if (hitsById.containsKey(version.getId())) {
                    scores.putAll(hitsById.get(version.getId()).getChannelScores());
                }
                result.add(new FactCandidate(key, version, true, scores, result.size() + 1));
            }
        }
        int softCount = 0;
        for (Bm25Index.Hit hit : hits) {
            FactKey candidateKey = keys.get(hit.getId());
            if (!key.equals(candidateKey)) {
                result.add(new FactCandidate(candidateKey, byId.get(hit.getId()), false,
                    hit.getChannelScores(), result.size() + 1));
                if (++softCount == topK) {
                    break;
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    private Map<String, Map<String, String>> documents(List<MemoryFactVersion> versions) {
        Map<String, Map<String, String>> documents = new TreeMap<>();
        for (MemoryFactVersion version : versions) {
            MemoryFact fact = version.getFact();
            Map<String, String> fields = new TreeMap<>();
            fields.put("subject", fact.getSubject().getId() + " " + fact.getSubject().getLabel());
            fields.put("predicate", fact.getPredicate());
            fields.put("object", fact.isRelationship()
                ? fact.getTarget().get().getId() + " " + fact.getTarget().get().getLabel()
                : fact.getLiteralValue().get());
            fields.put("evidence", version.getEvidence().stream()
                .map(Evidence::getContent).collect(Collectors.joining(" ")));
            documents.put(version.getId(), fields);
        }
        return documents;
    }

    private List<FactCandidate> strategyFacts(Map<String, Map<String, String>> documents,
                                               List<MemoryFactVersion> visible,
                                               Map<String, MemoryFactVersion> byId,
                                               Map<String, FactKey> keys, FactKey key,
                                               String text, int topK) throws IOException {
        final Map<String, List<Bm25Index.Hit>> channels = new TreeMap<>();
        final int poolSize = Math.max(1, documents.size());
        if (profile.usesBm25()) {
            try (Bm25Index index = new Bm25Index(documents, profile)) {
                channels.put("BM25", index.search(text, poolSize));
            }
        }
        if (profile.usesMinHash()) {
            channels.put("MINHASH", new MinHashIndex(documents).search(text, poolSize));
        }
        TreeSet<String> mandatoryIds = new TreeSet<>();
        for (MemoryFactVersion version : visible) {
            if (key.equals(keys.get(version.getId()))) {
                mandatoryIds.add(version.getId());
            }
        }
        List<FactCandidate> result = new ArrayList<>();
        for (Bm25Index.Hit hit : CandidateMerger.merge(channels, mandatoryIds, topK)) {
            result.add(new FactCandidate(keys.get(hit.getId()), byId.get(hit.getId()),
                mandatoryIds.contains(hit.getId()), hit.getScore(), hit.getRank(),
                hit.getChannelScores(), hit.getChannelRanks()));
        }
        return Collections.unmodifiableList(result);
    }

    /** Evidence and corpus statistics come only from visible versions in key.scope. */
    public List<EvidenceCandidate> evidence(CanonicalSnapshot snapshot, QueryMode mode, FactKey key,
                                           Instant validAt, Instant systemAt, String text, int topK)
        throws IOException {
        validateRetrieval(text, topK);
        List<MemoryFactVersion> visible = visible(snapshot, mode, key, validAt, systemAt,
            versionKeys(snapshot));
        Map<String, Evidence> corpus = new TreeMap<>();
        Map<String, List<String>> versionIds = new TreeMap<>();
        for (MemoryFactVersion version : visible) {
            for (Evidence evidence : version.getEvidence()) {
                Evidence previous = corpus.put(evidence.getId(), evidence);
                if (previous != null && !previous.equals(evidence)) {
                    throw new IllegalArgumentException("Evidence ID reused: " + evidence.getId());
                }
                versionIds.computeIfAbsent(evidence.getId(), ignored -> new ArrayList<>())
                    .add(version.getId());
            }
        }
        List<EvidenceCandidate> result = new ArrayList<>();
        try (EvidenceRetriever retriever = new EvidenceRetriever(corpus.values(), profile)) {
            for (EvidenceCandidate candidate : retriever.search(text, topK)) {
                result.add(new EvidenceCandidate(candidate.getEvidence(),
                    versionIds.get(candidate.getEvidence().getId()),
                    candidate.getScore(), candidate.getRank(), candidate.getChannelScores(),
                    candidate.getChannelRanks()));
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Use the committed prefix before applying the event; events select overlapping intervals. */
    public List<FactCandidate> eventFacts(CanonicalSnapshot snapshot, NormalizedMemoryEvent event) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(event, "event");
        List<FactCandidate> result = new ArrayList<>();
        List<MemoryFactVersion> versions = new ArrayList<>(snapshot.getState()
            .getVersionsByFactKey().getOrDefault(event.getFactKey(), Collections.emptyList()));
        versions.sort(Comparator.comparing(MemoryFactVersion::getId));
        for (MemoryFactVersion version : versions) {
            if (version.getStatus() == MemoryFactVersionStatus.ACTIVE
                && version.getTransactionTime().contains(event.getRecordedAt())
                && version.getValidTime().overlaps(event.getValidTime())) {
                result.add(new FactCandidate(event.getFactKey(), version, true,
                    Collections.singletonMap("RULE", 1.0), result.size() + 1));
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Retrieval explains a resolved event against its actual committed prefix only. */
    public List<FactCandidate> eventFacts(TemporalState state, NormalizedMemoryEvent event,
                                         TimeInterval effectiveInterval, int topK)
        throws IOException {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(effectiveInterval, "effectiveInterval");
        if (!profile.isCandidateStrategy()) {
            throw new IllegalArgumentException("Event retrieval requires a RULE candidate profile");
        }
        String text = eventText(event);
        validateRetrieval(text, topK);
        final Map<String, FactKey> keys = new HashMap<>();
        final Map<String, MemoryFactVersion> byId = new HashMap<>();
        final List<MemoryFactVersion> visible = new ArrayList<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            if (!event.getFactKey().getScope().equals(entry.getKey().getScope())) {
                continue;
            }
            for (MemoryFactVersion version : entry.getValue()) {
                if (version.getStatus() == MemoryFactVersionStatus.ACTIVE
                    && version.getTransactionTime().contains(event.getRecordedAt())
                    && version.getValidTime().overlaps(effectiveInterval)) {
                    keys.put(version.getId(), entry.getKey());
                    byId.put(version.getId(), version);
                    visible.add(version);
                }
            }
        }
        return strategyFacts(documents(visible), visible, byId, keys,
            event.getFactKey(), text, topK);
    }

    private static String eventText(NormalizedMemoryEvent event) {
        StringBuilder text = new StringBuilder(event.getFactKey().getSubjectId())
            .append(' ').append(event.getFactKey().getPredicate());
        if (event.getEvent().getFact().isPresent()) {
            MemoryFact fact = event.getEvent().getFact().get();
            text.append(' ').append(fact.getSubject().getLabel()).append(' ')
                .append(fact.isRelationship()
                    ? fact.getTarget().get().getId() + " " + fact.getTarget().get().getLabel()
                    : fact.getLiteralValue().get());
        }
        for (Evidence evidence : event.getEvidence()) {
            text.append(' ').append(evidence.getContent());
        }
        return text.toString();
    }

    private List<MemoryFactVersion> visible(CanonicalSnapshot snapshot, QueryMode mode, FactKey key,
                                            Instant validAt, Instant systemAt,
                                            Map<String, FactKey> keys) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(key, "key");
        if (mode != QueryMode.AS_OF && systemAt != null) {
            throw new IllegalArgumentException("systemAt is only valid for AS_OF");
        }
        List<MemoryFactVersion> versions;
        switch (mode) {
            case CURRENT:
                versions = query.current(snapshot.getState(), validAt);
                break;
            case HISTORICAL:
                versions = query.historical(snapshot.getState(), validAt);
                break;
            case AS_OF:
                versions = query.asOf(snapshot.getState(), validAt, systemAt);
                break;
            default:
                throw new IllegalArgumentException("Unsupported query mode: " + mode);
        }
        return versions.stream()
            .filter(version -> key.getScope().equals(keys.get(version.getId()).getScope()))
            .collect(Collectors.toList());
    }

    private static Map<String, FactKey> versionKeys(CanonicalSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        final Map<String, FactKey> keys = new HashMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : snapshot.getState().getVersionsByFactKey().entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                keys.put(version.getId(), entry.getKey());
            }
        }
        return keys;
    }

    private static void validateRetrieval(String text, int topK) {
        Objects.requireNonNull(text, "text");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
    }
}
