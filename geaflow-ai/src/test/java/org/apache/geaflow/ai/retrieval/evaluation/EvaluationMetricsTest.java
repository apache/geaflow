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
 */
package org.apache.geaflow.ai.retrieval.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class EvaluationMetricsTest {
    @Test
    void normalizesAnswersAndComputesOverlap() {
        assertEquals("confucius teacher", AnswerMetrics.normalize("Confucius, TEACHER!"));
        assertEquals(1.0D, AnswerMetrics.f1("A B", "b a"));
        org.junit.jupiter.api.Assertions.assertTrue(AnswerMetrics.exactMatch("Paris!", "paris"));
    }

    @Test
    void computesRankingMetricsAndPathHits() {
        List<String> ranked = Arrays.asList("x", "gold", "other");
        HashSet<String> relevant = new HashSet<>(Arrays.asList("gold"));
        assertEquals(1.0D, RankingMetrics.recallAtK(ranked, relevant, 2));
        assertEquals(0.5D, RankingMetrics.reciprocalRank(ranked, relevant));
        assertEquals(0.6309297535714574D, RankingMetrics.ndcgAtK(ranked, relevant, 3));
        assertEquals(1.0D, RankingMetrics.relationPathHit(
            new HashSet<>(Arrays.asList("a-b-c")), new HashSet<>(Arrays.asList("a-b-c"))));
    }

    @Test
    void calculatesStablePercentiles() {
        assertEquals(2L, Percentile.calculate(Arrays.asList(3L, 1L, 2L), 0.5D));
        assertEquals(3L, Percentile.calculate(Arrays.asList(3L, 1L, 2L), 0.95D));
    }
}
