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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.Evidence;

/** Evidence-only retrieval, also used by the independent public-data evaluation. */
public final class EvidenceRetriever implements AutoCloseable {

    private final Map<String, Evidence> evidenceById = new TreeMap<>();
    private final Map<String, List<String>> exactIds = new TreeMap<>();
    private final RetrievalProfile profile;
    private final Bm25Index index;
    private final MinHashIndex minHash;
    private boolean closed;

    public EvidenceRetriever(Collection<Evidence> corpus) throws IOException {
        this(corpus, RetrievalProfile.BASELINE);
    }

    public EvidenceRetriever(Collection<Evidence> corpus, RetrievalProfile profile)
        throws IOException {
        Objects.requireNonNull(corpus, "corpus");
        this.profile = Objects.requireNonNull(profile, "profile");
        Map<String, Map<String, String>> documents = new TreeMap<>();
        for (Evidence evidence : corpus) {
            Objects.requireNonNull(evidence, "evidence");
            Evidence previous = evidenceById.put(evidence.getId(), evidence);
            if (previous != null && !previous.equals(evidence)) {
                throw new IllegalArgumentException("Evidence ID reused: " + evidence.getId());
            }
            Map<String, String> fields = new TreeMap<>();
            fields.put("evidence", evidence.getContent());
            documents.put(evidence.getId(), fields);
        }
        if (profile.isCandidateStrategy()) {
            for (Evidence evidence : evidenceById.values()) {
                String normalized = TextNormalizer.normalize(evidence.getContent());
                exactIds.computeIfAbsent(normalized, ignored -> new ArrayList<>())
                    .add(evidence.getId());
            }
        }
        minHash = profile.usesMinHash() ? new MinHashIndex(documents) : null;
        index = profile.usesBm25() ? new Bm25Index(documents, profile) : null;
    }

    public List<EvidenceCandidate> search(String text, int topK) throws IOException {
        if (closed) {
            throw new IllegalStateException("Retriever is closed");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        List<Bm25Index.Hit> hits;
        if (profile.isCandidateStrategy()) {
            final String normalized = TextNormalizer.normalize(text);
            final int poolSize = profile.getCandidatePoolSize(topK);
            Map<String, List<Bm25Index.Hit>> channels = new TreeMap<>();
            if (index != null) {
                channels.put("BM25", index.search(text, poolSize));
            }
            if (minHash != null) {
                channels.put("MINHASH", minHash.search(text, poolSize));
            }
            List<String> rules = normalized.isEmpty() ? Collections.emptyList()
                : exactIds.getOrDefault(normalized, Collections.emptyList());
            List<Bm25Index.Hit> merged = CandidateMerger.merge(channels, rules, topK);
            hits = merged.subList(0, Math.min(topK, merged.size()));
        } else {
            hits = index.search(text, topK);
        }
        List<EvidenceCandidate> result = new ArrayList<>();
        for (Bm25Index.Hit hit : hits) {
            result.add(new EvidenceCandidate(evidenceById.get(hit.getId()),
                Collections.emptyList(), hit.getScore(), hit.getRank(),
                hit.getChannelScores(), hit.getChannelRanks()));
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (index != null) {
                index.close();
            }
        }
    }
}
