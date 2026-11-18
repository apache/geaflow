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

package org.apache.geaflow.ai.retrieval.execution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;

/** Stable default reranker; graph path quality is an explicit secondary feature. */
public final class DeterministicReranker implements Reranker {
    public static final String VERSION = "deterministic-reranker-v1";
    private final double graphPathWeight;

    public DeterministicReranker() {
        this(0.0);
    }

    public DeterministicReranker(double graphPathWeight) {
        if (!Double.isFinite(graphPathWeight) || graphPathWeight < 0.0) {
            throw new IllegalArgumentException("graphPathWeight must be finite and non-negative");
        }
        this.graphPathWeight = graphPathWeight;
    }

    @Override
    public List<Evidence> rerank(List<Evidence> candidates) {
        List<Evidence> result = new ArrayList<>(candidates == null ? java.util.Collections.emptyList() : candidates);
        result.sort(Comparator.comparingDouble((Evidence evidence) -> rerankScore(evidence)).reversed()
            .thenComparing(Evidence::getEvidenceId));
        return result;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    private double rerankScore(Evidence evidence) {
        double fused = evidence.getFusedScore() == null ? 0.0 : evidence.getFusedScore();
        return fused + graphPathWeight * pathQuality(evidence);
    }

    private static double pathQuality(Evidence evidence) {
        double quality = 0.0;
        for (GraphPathRef path : evidence.getPaths()) {
            quality = Math.max(quality, 1.0 / Math.max(1, path.getHop()));
        }
        return quality;
    }
}
