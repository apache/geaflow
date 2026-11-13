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
package org.apache.geaflow.ai.retrieval.channel.graph;

/**
 * Deterministic edge-strength model combining a relation prior, incremental reinforcement, and an
 * optional time decay.
 *
 * <p>The design states that edge strength "may combine a relation-type prior, document co-occurrence
 * frequency, repeated incremental reinforcement, and an optional time-decay factor", and that these
 * components and their versions must be emitted in build metadata. Only the relation prior was
 * implemented; co-occurrence frequency, reinforcement, and time decay were missing, so a graph that
 * had been reinforced or partially refreshed scored identically to a fresh one.
 *
 * <p>The model here is:
 *
 * <pre>
 * edgeStrength = prior
 *              * (1 + reinforcementWeight * log(1 + reinforcementCount))
 *              * timeDecay^(age / halfLife)
 * </pre>
 *
 * <p>All three components are neutral by default: a zero reinforcement count and a zero configured
 * time-decay weight both leave {@code edgeStrength == prior}, so enabling this model does not move
 * any previously published score.
 */
public final class EdgeStrength {

    /**
     * Version of the edge-strength model implemented here, recorded as {@code edgeStrengthVersion}.
     */
    public static final String EDGE_STRENGTH_VERSION = "relation-prior-v2";

    /**
     * Default time-decay weight: {@code 0.0} keeps the term disabled so every previously published
     * score is reproduced bit for bit until a build opts in.
     */
    public static final double DEFAULT_TIME_DECAY_WEIGHT = 0.0D;

    /** Default half-life in milliseconds, used only once a nonzero decay weight is configured. */
    public static final double DEFAULT_TIME_DECAY_HALF_LIFE_MILLIS = 86_400_000.0D;

    private EdgeStrength() {
    }

    /**
     * Returns the neutral edge strength for a relation label: just the configured prior.
     *
     * @param prior configured relation-type prior
     * @return {@code prior}
     */
    public static double of(double prior) {
        return of(prior, 0, 0L, 0.0D, Double.POSITIVE_INFINITY);
    }

    /**
     * Combines the relation prior, incremental reinforcement, and time decay.
     *
     * @param prior configured relation-type prior, non-negative
     * @param reinforcementCount number of times this edge was reinforced by incremental ingestion
     * @param ageMillis observed age of the edge in milliseconds, non-negative
     * @param halfLifeMillis half-life of the time decay; {@code <= 0} or infinite disables decay
     * @param timeDecayWeight in {@code [0,1]}; {@code 0} disables decay, {@code 1} applies it fully
     * @return the combined edge strength
     */
    public static double of(double prior, int reinforcementCount, long ageMillis,
                            double timeDecayWeight, double halfLifeMillis) {
        if (!Double.isFinite(prior) || prior < 0.0D) {
            throw new IllegalArgumentException("prior must be finite and non-negative");
        }
        if (reinforcementCount < 0) {
            throw new IllegalArgumentException("reinforcementCount must be non-negative");
        }
        if (ageMillis < 0L) {
            throw new IllegalArgumentException("ageMillis must be non-negative");
        }
        if (!Double.isFinite(timeDecayWeight) || timeDecayWeight < 0.0D || timeDecayWeight > 1.0D) {
            throw new IllegalArgumentException("timeDecayWeight must be in [0,1]");
        }
        return prior * reinforcementFactor(reinforcementCount) * decayFactor(ageMillis, timeDecayWeight,
            halfLifeMillis);
    }

    /**
     * Returns the multiplicative reinforcement factor.
     *
     * <p>A count of zero is neutral; reinforcement grows logarithmically so heavily reinforced
     * edges cannot dominate the frontier on frequency alone.
     *
     * @param reinforcementCount number of reinforcements, non-negative
     * @return {@code 1 + log(1 + count)} normalized so zero reinforcements yields {@code 1}
     */
    public static double reinforcementFactor(int reinforcementCount) {
        if (reinforcementCount < 0) {
            throw new IllegalArgumentException("reinforcementCount must be non-negative");
        }
        return 1.0D + Math.log1p(reinforcementCount) / Math.log(2.0D);
    }

    /**
     * Returns the multiplicative time-decay factor in {@code (0,1]}.
     *
     * @param ageMillis observed age in milliseconds, non-negative
     * @param timeDecayWeight in {@code [0,1]}; {@code 0} disables decay
     * @param halfLifeMillis half-life; values {@code <= 0} or infinite disable decay
     * @return the decay factor, exactly {@code 1.0} when decay is disabled
     */
    public static double decayFactor(long ageMillis, double timeDecayWeight, double halfLifeMillis) {
        if (ageMillis < 0L) {
            throw new IllegalArgumentException("ageMillis must be non-negative");
        }
        if (!Double.isFinite(timeDecayWeight) || timeDecayWeight < 0.0D || timeDecayWeight > 1.0D) {
            throw new IllegalArgumentException("timeDecayWeight must be in [0,1]");
        }
        if (timeDecayWeight == 0.0D || Double.isInfinite(halfLifeMillis) || halfLifeMillis <= 0.0D) {
            return 1.0D;
        }
        double remaining = Math.pow(0.5D, ageMillis / halfLifeMillis);
        return 1.0D - timeDecayWeight * (1.0D - remaining);
    }
}