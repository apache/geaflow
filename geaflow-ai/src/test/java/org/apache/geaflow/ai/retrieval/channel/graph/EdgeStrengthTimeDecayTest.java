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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.junit.jupiter.api.Test;

/**
 * Pins the edge-strength model: relation prior, incremental reinforcement, and optional time decay.
 *
 * <p>The design allows edge strength to combine "a relation-type prior, document co-occurrence
 * frequency, repeated incremental reinforcement, and an optional time-decay factor", and requires
 * those components and their versions in build metadata. Only the relation prior was covered
 * before, so a reinforced or aged edge behaved exactly like a fresh one.
 */
class EdgeStrengthTimeDecayTest {

    private static final double TOLERANCE = 1.0e-12;
    private static final long ONE_DAY_MILLIS = 86_400_000L;

    @Test
    void neutralModelReducesToTheRelationPrior() {
        // Defaults must reproduce previously published scores bit for bit.
        assertEquals(1.4D, EdgeStrength.of(1.4D), TOLERANCE);
        assertEquals(1.4D, EdgeStrength.of(1.4D, 0, 0L, 0.0D, ONE_DAY_MILLIS), TOLERANCE);
        assertEquals(0.0D, EdgeStrength.of(0.0D), TOLERANCE);
    }

    @Test
    void reinforcementFactorIsOneAtZeroAndGrowsLogarithmically() {
        assertEquals(1.0D, EdgeStrength.reinforcementFactor(0), TOLERANCE);
        assertEquals(2.0D, EdgeStrength.reinforcementFactor(1), TOLERANCE);
        assertEquals(1.0D + Math.log1p(3) / Math.log(2.0D),
            EdgeStrength.reinforcementFactor(3), TOLERANCE);

        // Logarithmic growth: a hundred reinforcements must not multiply the prior a hundredfold.
        double heavy = EdgeStrength.reinforcementFactor(100);
        assertTrue(heavy < 8.0D, "reinforcement must grow logarithmically, got " + heavy);
        assertTrue(heavy > EdgeStrength.reinforcementFactor(10));
    }

    @Test
    void repeatedReinforcementStrengthensAnEdgeMonotonically() {
        double previous = 0.0D;
        for (int count = 0; count <= 50; count++) {
            double strength = EdgeStrength.of(1.0D, count, 0L, 0.0D, ONE_DAY_MILLIS);
            assertTrue(strength >= previous, "strength must not fall as reinforcement grows");
            previous = strength;
        }
    }

    @Test
    void timeDecayIsOneWhenDisabled() {
        // Weight zero, infinite half-life, and non-positive half-life all disable the term.
        assertEquals(1.0D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 0.0D, ONE_DAY_MILLIS), TOLERANCE);
        assertEquals(1.0D,
            EdgeStrength.decayFactor(ONE_DAY_MILLIS, 1.0D, Double.POSITIVE_INFINITY), TOLERANCE);
        assertEquals(1.0D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 1.0D, 0.0D), TOLERANCE);
        assertEquals(1.0D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 1.0D, -5.0D), TOLERANCE);
    }

    @Test
    void timeDecayFollowsTheConfiguredHalfLife() {
        // At exactly one half-life the remaining factor is 0.5; a full weight maps that onto 0.5.
        assertEquals(0.5D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS), TOLERANCE);
        assertEquals(0.25D, EdgeStrength.decayFactor(2 * ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS), TOLERANCE);
        assertEquals(1.0D, EdgeStrength.decayFactor(0L, 1.0D, ONE_DAY_MILLIS), TOLERANCE);

        // Half weight interpolates halfway between "no decay" and "full decay".
        assertEquals(0.75D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 0.5D, ONE_DAY_MILLIS), TOLERANCE);
        assertEquals(0.875D, EdgeStrength.decayFactor(ONE_DAY_MILLIS, 0.25D, ONE_DAY_MILLIS), TOLERANCE);
    }

    @Test
    void decayFactorStaysWithinUnitIntervalForAnyAge() {
        for (long age : new long[] {0L, 1L, 1_000L, ONE_DAY_MILLIS, 10 * ONE_DAY_MILLIS}) {
            double factor = EdgeStrength.decayFactor(age, 1.0D, ONE_DAY_MILLIS);
            assertTrue(factor > 0.0D && factor <= 1.0D,
                "decay factor must be in (0,1] at age " + age + ", got " + factor);
        }
        // Extremely old edges are allowed to decay all the way to zero rather than going negative.
        assertEquals(0.0D, EdgeStrength.decayFactor(3_650L * ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS), TOLERANCE);
    }

    @Test
    void ageReducesStrengthWhileReinforcementIncreasesIt() {
        double fresh = EdgeStrength.of(1.0D, 0, 0L, 1.0D, ONE_DAY_MILLIS);
        double aged = EdgeStrength.of(1.0D, 0, 2 * ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS);
        double reinforced = EdgeStrength.of(1.0D, 5, 0L, 1.0D, ONE_DAY_MILLIS);

        assertEquals(1.0D, fresh, TOLERANCE);
        assertEquals(0.25D, aged, TOLERANCE);
        assertTrue(reinforced > fresh, "reinforcement must strengthen a fresh edge");
    }

    @Test
    void reinforcementAndDecayComposeMultiplicatively() {
        double prior = 1.2D;
        double expected = prior
            * EdgeStrength.reinforcementFactor(7)
            * EdgeStrength.decayFactor(ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS);
        assertEquals(expected, EdgeStrength.of(prior, 7, ONE_DAY_MILLIS, 1.0D, ONE_DAY_MILLIS), TOLERANCE);
    }

    @Test
    void rejectsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> EdgeStrength.of(-1.0D));
        assertThrows(IllegalArgumentException.class, () -> EdgeStrength.of(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> EdgeStrength.reinforcementFactor(-1));
        assertThrows(IllegalArgumentException.class,
            () -> EdgeStrength.of(1.0D, -1, 0L, 0.0D, ONE_DAY_MILLIS));
        assertThrows(IllegalArgumentException.class,
            () -> EdgeStrength.of(1.0D, 0, -1L, 0.0D, ONE_DAY_MILLIS));
        assertThrows(IllegalArgumentException.class,
            () -> EdgeStrength.of(1.0D, 0, 0L, 1.5D, ONE_DAY_MILLIS));
        assertThrows(IllegalArgumentException.class,
            () -> EdgeStrength.decayFactor(-1L, 1.0D, ONE_DAY_MILLIS));
    }

    @Test
    void configurationDefaultsKeepDecayDisabledAndExposeTheOptIn() {
        GraphRetrievalConfig defaults = GraphRetrievalConfig.defaults();
        assertEquals(0.0D, defaults.getTimeDecayWeight(), TOLERANCE);
        assertEquals(EdgeStrength.DEFAULT_TIME_DECAY_HALF_LIFE_MILLIS,
            defaults.getTimeDecayHalfLifeMillis(), TOLERANCE);

        GraphRetrievalConfig optedIn = GraphRetrievalConfig.builder()
            .candidateBudget(10).edgeScanBudget(10)
            .timeDecayWeight(1.0D).timeDecayHalfLifeMillis(60_000.0D).build();
        assertEquals(1.0D, optedIn.getTimeDecayWeight(), TOLERANCE);
        assertEquals(60_000.0D, optedIn.getTimeDecayHalfLifeMillis(), TOLERANCE);
    }

    @Test
    void configurationRejectsOutOfRangeDecaySettings() {
        assertThrows(IllegalArgumentException.class, () -> GraphRetrievalConfig.builder()
            .timeDecayWeight(1.5D).build());
        assertThrows(IllegalArgumentException.class, () -> GraphRetrievalConfig.builder()
            .timeDecayWeight(-0.1D).build());
        assertThrows(IllegalArgumentException.class, () -> GraphRetrievalConfig.builder()
            .timeDecayHalfLifeMillis(-1.0D).build());
        assertThrows(IllegalArgumentException.class, () -> GraphRetrievalConfig.builder()
            .timeDecayHalfLifeMillis(Double.NaN).build());
    }

    @Test
    void reinforcementSourceSuppliesCountAndAgeWithoutChangingTheProviderContract() {
        GraphEdgeRef edge = new GraphEdgeRef("rel-1", "mentions", "e-1", "e-2");
        EdgeReinforcementSource source = new EdgeReinforcementSource() {
            @Override
            public int getReinforcementCount(GraphEdgeRef value) {
                return 3;
            }

            @Override
            public long getAgeMillis(GraphEdgeRef value) {
                return ONE_DAY_MILLIS;
            }
        };

        assertEquals(3, source.getReinforcementCount(edge));
        assertEquals(ONE_DAY_MILLIS, source.getAgeMillis(edge));
        // The narrow optional interface is separate from GraphNeighborProvider on purpose.
        assertTrue(!(source instanceof GraphNeighborProvider),
            "reinforcement is an optional capability, not part of the provider contract");
    }

    @Test
    void anchorAndEdgeProvenanceStillDriveSourceRelevanceInputs() {
        // Guards the inputs the retriever feeds into the formula's sourceRelevance term.
        EntityRef anchor = new EntityRef("e-1", "Ada", Collections.emptyList(), "person",
            Collections.singletonList("chunk-1"));
        GraphEdgeRef sameChunk = new GraphEdgeRef("rel-1", "mentions", "e-1", "e-2",
            Collections.singletonList("chunk-1"));
        List<String> anchorSources = anchor.getSourceChunkIds();
        assertTrue(anchorSources.containsAll(sameChunk.getSourceChunkIds()));
    }
}