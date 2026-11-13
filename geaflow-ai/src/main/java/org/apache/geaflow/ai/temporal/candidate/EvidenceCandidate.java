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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.model.Evidence;

public final class EvidenceCandidate {

    private final Evidence evidence;
    private final List<String> versionIds;
    private final double score;
    private final int rank;
    private final Map<String, Double> channelScores;
    private final Map<String, Integer> channelRanks;

    public EvidenceCandidate(Evidence evidence, List<String> versionIds, double score, int rank) {
        this(evidence, versionIds, score, rank, Collections.singletonMap("BM25", score),
            Collections.singletonMap("BM25", rank));
    }

    public EvidenceCandidate(Evidence evidence, List<String> versionIds, double score, int rank,
                             Map<String, Double> channelScores,
                             Map<String, Integer> channelRanks) {
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        TreeSet<String> ordered = new TreeSet<>();
        for (String id : Objects.requireNonNull(versionIds, "versionIds")) {
            if (Objects.requireNonNull(id, "versionId").trim().isEmpty()) {
                throw new IllegalArgumentException("versionId must not be blank");
            }
            ordered.add(id);
        }
        if (!Double.isFinite(score) || score < 0 || rank <= 0) {
            throw new IllegalArgumentException(
                "score must be finite and nonnegative; rank positive");
        }
        this.versionIds = Collections.unmodifiableList(new ArrayList<>(ordered));
        this.score = score;
        this.rank = rank;
        Map<String, Double> scores = new TreeMap<>();
        for (Map.Entry<String, Double> entry
            : Objects.requireNonNull(channelScores, "channelScores").entrySet()) {
            String channel = Objects.requireNonNull(entry.getKey(), "channel");
            double value = Objects.requireNonNull(entry.getValue(), "channel score");
            if (channel.trim().isEmpty() || !Double.isFinite(value) || value < 0) {
                throw new IllegalArgumentException("Invalid evidence channel score");
            }
            scores.put(channel, value);
        }
        Map<String, Integer> ranks = new TreeMap<>();
        for (Map.Entry<String, Integer> entry
            : Objects.requireNonNull(channelRanks, "channelRanks").entrySet()) {
            String channel = Objects.requireNonNull(entry.getKey(), "channel");
            int value = Objects.requireNonNull(entry.getValue(), "channel rank");
            if (!scores.containsKey(channel) || value <= 0) {
                throw new IllegalArgumentException("Invalid evidence channel rank");
            }
            ranks.put(channel, value);
        }
        this.channelScores = Collections.unmodifiableMap(scores);
        this.channelRanks = Collections.unmodifiableMap(ranks);
    }

    public Evidence getEvidence() {
        return evidence;
    }

    public List<String> getVersionIds() {
        return versionIds;
    }

    public double getScore() {
        return score;
    }

    public int getRank() {
        return rank;
    }

    public Map<String, Double> getChannelScores() {
        return channelScores;
    }

    public Map<String, Integer> getChannelRanks() {
        return channelRanks;
    }
}
