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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Deterministic RRF over independent rankings, with complete mandatory candidates first. */
public final class CandidateMerger {

    private CandidateMerger() {
    }

    public static List<Bm25Index.Hit> merge(Map<String, List<Bm25Index.Hit>> channels,
                                            Collection<String> mandatoryIds, int optionalTopK) {
        Objects.requireNonNull(channels, "channels");
        Objects.requireNonNull(mandatoryIds, "mandatoryIds");
        if (optionalTopK <= 0) {
            throw new IllegalArgumentException("optionalTopK must be positive");
        }
        final Map<String, Candidate> byId = new TreeMap<>();
        for (Map.Entry<String, List<Bm25Index.Hit>> entry : new TreeMap<>(channels).entrySet()) {
            String channel = validName(entry.getKey(), "channel");
            if ("RULE".equals(channel) || "RRF".equals(channel)) {
                throw new IllegalArgumentException("Reserved candidate channel: " + channel);
            }
            final Set<String> seenIds = new HashSet<>();
            final Set<Integer> seenRanks = new HashSet<>();
            for (Bm25Index.Hit hit : Objects.requireNonNull(entry.getValue(), "channel hits")) {
                Objects.requireNonNull(hit, "hit");
                String id = validName(hit.getId(), "candidate ID");
                if (!Double.isFinite(hit.getScore()) || hit.getScore() < 0 || hit.getRank() <= 0
                    || !seenIds.add(id) || !seenRanks.add(hit.getRank())) {
                    throw new IllegalArgumentException("Invalid or duplicate candidate hit");
                }
                Candidate candidate = byId.computeIfAbsent(id, Candidate::new);
                candidate.add(channel, hit.getScore(), hit.getRank());
            }
        }
        final TreeSet<String> mandatory = new TreeSet<>();
        for (String id : mandatoryIds) {
            mandatory.add(validName(id, "mandatory ID"));
        }
        int ruleRank = 0;
        for (String id : mandatory) {
            byId.computeIfAbsent(id, Candidate::new).add("RULE", 1.0, ++ruleRank);
        }
        final List<Candidate> ordered = new ArrayList<>(byId.values());
        ordered.sort(Comparator.comparingDouble((Candidate candidate) -> candidate.score)
            .reversed().thenComparing(candidate -> candidate.id));
        for (int i = 0; i < ordered.size(); i++) {
            Candidate candidate = ordered.get(i);
            candidate.scores.put("RRF", candidate.score);
            candidate.ranks.put("RRF", i + 1);
        }
        final List<Bm25Index.Hit> result = new ArrayList<>();
        for (Candidate candidate : ordered) {
            if (mandatory.contains(candidate.id)) {
                result.add(candidate.hit(result.size() + 1));
            }
        }
        int optionalCount = 0;
        for (Candidate candidate : ordered) {
            if (!mandatory.contains(candidate.id) && optionalCount < optionalTopK) {
                result.add(candidate.hit(result.size() + 1));
                optionalCount++;
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static String validName(String value, String name) {
        if (Objects.requireNonNull(value, name).trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static final class Candidate {

        private final String id;
        private final Map<String, Double> scores = new TreeMap<>();
        private final Map<String, Integer> ranks = new TreeMap<>();
        private double score;

        private Candidate(String id) {
            this.id = id;
        }

        private void add(String channel, double channelScore, int rank) {
            scores.put(channel, channelScore);
            ranks.put(channel, rank);
            score += 1.0 / (RetrievalProfile.RRF_K + (double) rank);
        }

        private Bm25Index.Hit hit(int rank) {
            return new Bm25Index.Hit(id, score, rank, scores, ranks);
        }
    }
}
