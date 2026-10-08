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
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;

/** Deterministic weighted RRF ordering for BM25, vector, and graph evidence. */
public final class WeightedRrfFusion {
    private final WeightedRrfConfig config;

    public WeightedRrfFusion(WeightedRrfConfig config) {
        this.config = java.util.Objects.requireNonNull(config, "config");
    }

    public WeightedRrfConfig getConfig() {
        return config;
    }

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

    public List<Evidence> sort(List<Evidence> evidence) {
        List<Evidence> result = new ArrayList<>();
        for (Evidence value : evidence) {
            result.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(),
                value.getChunks(), value.getEntities(), value.getPaths(), value.getSources(),
                value.getStageScores(), score(value), value.getFinalScore(), value.getRank(),
                value.getGraphTraces()));
        }
        result.sort(Comparator.comparingDouble(Evidence::getFusedScore).reversed()
            .thenComparing(Evidence::getEvidenceId));
        List<Evidence> ranked = new ArrayList<>();
        for (Evidence value : result) {
            ranked.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(), value.getChunks(),
                value.getEntities(), value.getPaths(), value.getSources(), value.getStageScores(), value.getFusedScore(),
                value.getFinalScore(), ranked.size() + 1, value.getGraphTraces()));
        }
        return ranked;
    }
}
