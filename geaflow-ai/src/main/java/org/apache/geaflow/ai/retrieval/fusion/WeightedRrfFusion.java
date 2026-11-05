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
package org.apache.geaflow.ai.retrieval.fusion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Deterministic weighted RRF ordering for BM25, vector, and graph evidence. */
public final class WeightedRrfFusion implements Fusion {
    private final WeightedRrfConfig config;

    public WeightedRrfFusion(WeightedRrfConfig config) {
        this.config = java.util.Objects.requireNonNull(config, "config");
    }

    @Override
    public String getVersion() {
        return "weighted-rrf-v1";
    }

    @Override
    public WeightedRrfConfig getConfig() {
        return config;
    }

    @Override
    public double score(Evidence evidence) {
        double result = 0.0;
        for (ChannelScore score : evidence.getStageScores().values()) {
            double weight = "BM25".equals(score.getChannel()) ? config.getBm25Weight()
                : "VECTOR".equals(score.getChannel()) ? config.getVectorWeight()
                : "GRAPH".equals(score.getChannel()) ? config.getGraphWeight() : 0.0;
            if (weight > 0.0) {
                result += weight / ((double) config.getRankConstant() + score.getRank());
            }
        }
        return result;
    }

    @Override
    public List<Evidence> sort(List<Evidence> evidence) {
        List<Evidence> result = new ArrayList<>();
        for (Evidence value : evidence) {
            result.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(),
                value.getChunks(), value.getEntities(), value.getPaths(), value.getSources(),
                value.getStageScores(), score(value), value.getFinalScore(), value.getRank(),
                value.getGraphTraces(), value.getSchemaVersion(), value.getDataset(),
                value.getDatasetVersion(), value.getSplit(), value.getGraphVersion(),
                value.getIndexVersion()));
        }
        result.sort(Comparator.comparingDouble(Evidence::getFusedScore).reversed()
            .thenComparing(Evidence::getEvidenceId));
        List<Evidence> ranked = new ArrayList<>();
        for (Evidence value : result) {
            ranked.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(), value.getChunks(),
                value.getEntities(), value.getPaths(), value.getSources(), value.getStageScores(), value.getFusedScore(),
                value.getFinalScore(), ranked.size() + 1, value.getGraphTraces(),
                value.getSchemaVersion(), value.getDataset(), value.getDatasetVersion(),
                value.getSplit(), value.getGraphVersion(), value.getIndexVersion()));
        }
        return ranked;
    }

    /** Reserves a bounded final top-K quota for a channel when it has sufficient evidence. */
    @Override
    public List<Evidence> preserveTopKChannel(List<Evidence> ranked, int topK, String channel,
                                              int minimumCount) {
        if (topK < 1 || channel == null || channel.trim().isEmpty() || minimumCount < 1) {
            throw new IllegalArgumentException("topK, channel, and minimumCount are required");
        }
        List<Evidence> values = new ArrayList<>(ranked == null ? java.util.Collections.emptyList() : ranked);
        int quota = Math.min(topK, minimumCount);
        if (values.size() <= topK || countChannel(values.subList(0, Math.min(topK, values.size())), channel)
            >= quota) {
            return values;
        }
        List<Evidence> head = new ArrayList<>();
        Set<String> selected = new HashSet<>();
        for (Evidence value : values) {
            if (value.getStageScores().containsKey(channel)) {
                head.add(value);
                selected.add(value.getEvidenceId());
                if (head.size() >= quota) {
                    break;
                }
            }
        }
        if (head.isEmpty()) {
            return values;
        }
        for (Evidence value : values) {
            if (head.size() >= topK) {
                break;
            }
            if (selected.add(value.getEvidenceId())) {
                head.add(value);
            }
        }
        head.sort(Comparator.comparingDouble((Evidence value) -> value.getFusedScore() == null
            ? 0.0D : value.getFusedScore()).reversed().thenComparing(Evidence::getEvidenceId));
        List<Evidence> result = new ArrayList<>(values.size());
        result.addAll(head);
        for (Evidence value : values) {
            if (selected.add(value.getEvidenceId())) {
                result.add(value);
            }
        }
        return result;
    }

    private static int countChannel(List<Evidence> values, String channel) {
        int count = 0;
        for (Evidence value : values) {
            if (value.getStageScores().containsKey(channel)) {
                count++;
            }
        }
        return count;
    }
}
