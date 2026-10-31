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

/** Validated fixed weights for the v1 weighted reciprocal rank fusion. */
public final class WeightedRrfConfig {
    public static final WeightedRrfConfig DEFAULT = new WeightedRrfConfig(1.0, 1.0, 1.0, 60);
    private final double bm25Weight;
    private final double vectorWeight;
    private final double graphWeight;
    private final int rankConstant;

    public WeightedRrfConfig(double bm25Weight, double vectorWeight, int rankConstant) {
        this(bm25Weight, vectorWeight, 0.0, rankConstant);
    }

    public WeightedRrfConfig(double bm25Weight, double vectorWeight, double graphWeight,
                             int rankConstant) {
        if (!Double.isFinite(bm25Weight) || !Double.isFinite(vectorWeight)
            || !Double.isFinite(graphWeight)
            || bm25Weight < 0.0 || vectorWeight < 0.0
            || graphWeight < 0.0
            || (bm25Weight == 0.0 && vectorWeight == 0.0 && graphWeight == 0.0)
            || rankConstant < 1) {
            throw new IllegalArgumentException("invalid weighted RRF configuration");
        }
        this.bm25Weight = bm25Weight;
        this.vectorWeight = vectorWeight;
        this.graphWeight = graphWeight;
        this.rankConstant = rankConstant;
    }

    public double getBm25Weight() {
        return bm25Weight;
    }

    public double getVectorWeight() {
        return vectorWeight;
    }

    public double getGraphWeight() {
        return graphWeight;
    }

    public int getRankConstant() {
        return rankConstant;
    }
}
