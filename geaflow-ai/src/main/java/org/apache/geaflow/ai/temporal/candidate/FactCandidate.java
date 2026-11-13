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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;

public final class FactCandidate {

    private final FactKey factKey;
    private final MemoryFactVersion version;
    private final boolean mandatory;
    private final Map<String, Double> scores;
    private final int rank;
    private final double score;
    private final Map<String, Integer> channelRanks;

    public FactCandidate(FactKey factKey, MemoryFactVersion version, boolean mandatory,
                         Map<String, Double> scores, int rank) {
        this(factKey, version, mandatory, scores.getOrDefault("RRF",
            scores.getOrDefault("BM25", scores.getOrDefault("RULE", 0.0))), rank,
            scores, Collections.emptyMap());
    }

    public FactCandidate(FactKey factKey, MemoryFactVersion version, boolean mandatory,
                         double score, int rank, Map<String, Double> scores,
                         Map<String, Integer> channelRanks) {
        this.factKey = Objects.requireNonNull(factKey, "factKey");
        this.version = Objects.requireNonNull(version, "version");
        if (!factKey.getSubjectId().equals(version.getFact().getSubject().getId())
            || !factKey.getPredicate().equals(version.getFact().getPredicate())) {
            throw new IllegalArgumentException("Fact key does not match candidate version");
        }
        if (rank <= 0 || !Double.isFinite(score) || score < 0) {
            throw new IllegalArgumentException("rank must be positive; score finite and nonnegative");
        }
        Map<String, Double> copy = new TreeMap<>();
        for (Map.Entry<String, Double> entry
            : Objects.requireNonNull(scores, "scores").entrySet()) {
            String channel = Objects.requireNonNull(entry.getKey(), "channel");
            double channelScore = Objects.requireNonNull(entry.getValue(), "score");
            if (channel.trim().isEmpty() || !Double.isFinite(channelScore) || channelScore < 0) {
                throw new IllegalArgumentException("Invalid candidate channel or score");
            }
            copy.put(channel, channelScore);
        }
        this.mandatory = mandatory;
        this.scores = Collections.unmodifiableMap(copy);
        this.rank = rank;
        this.score = score;
        Map<String, Integer> ranks = new TreeMap<>();
        for (Map.Entry<String, Integer> entry
            : Objects.requireNonNull(channelRanks, "channelRanks").entrySet()) {
            String channel = Objects.requireNonNull(entry.getKey(), "channel");
            int channelRank = Objects.requireNonNull(entry.getValue(), "channel rank");
            if (!copy.containsKey(channel) || channelRank <= 0) {
                throw new IllegalArgumentException("Invalid candidate channel rank");
            }
            ranks.put(channel, channelRank);
        }
        this.channelRanks = Collections.unmodifiableMap(ranks);
    }

    public FactKey getFactKey() {
        return factKey;
    }

    public MemoryFactVersion getVersion() {
        return version;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public Map<String, Double> getScores() {
        return scores;
    }

    public int getRank() {
        return rank;
    }

    public double getScore() {
        return score;
    }

    public Map<String, Integer> getChannelRanks() {
        return channelRanks;
    }
}
