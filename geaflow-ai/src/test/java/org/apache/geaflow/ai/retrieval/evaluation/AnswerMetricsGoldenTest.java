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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pins the answer metrics against a golden fixture shared with the Python evaluator.
 *
 * <p>The benchmark scores answers in Python while the Java module exposes equivalent helpers, so a
 * silent divergence would let the two report different EM/F1 for the same prediction. Both sides
 * read this fixture, which makes the parity explicit instead of assumed.
 */
class AnswerMetricsGoldenTest {
    private static final double TOLERANCE = 1.0e-9;

    @Test
    void matchesTheSharedGoldenFixture() {
        JsonObject golden = load();
        JsonArray cases = golden.getAsJsonArray("cases");
        assertEquals(8, cases.size(), "the golden fixture must keep its reviewed cases");

        for (int index = 0; index < cases.size(); index++) {
            JsonObject item = cases.get(index).getAsJsonObject();
            String prediction = item.get("prediction").getAsString();
            String answer = item.get("answer").getAsString();
            double expectedExactMatch = item.get("exactMatch").getAsDouble();
            double expectedF1 = item.get("f1").getAsDouble();

            assertEquals(expectedExactMatch,
                AnswerMetrics.exactMatch(prediction, answer) ? 1.0D : 0.0D, TOLERANCE,
                "exactMatch mismatch for '" + prediction + "' vs '" + answer + "'");
            assertEquals(expectedF1, AnswerMetrics.f1(prediction, answer), TOLERANCE,
                "f1 mismatch for '" + prediction + "' vs '" + answer + "'");
        }
    }

    private static JsonObject load() {
        try (Reader reader = new InputStreamReader(
            AnswerMetricsGoldenTest.class.getResourceAsStream("/answer/em-f1-golden.json"),
            StandardCharsets.UTF_8)) {
            return new JsonParser().parse(reader).getAsJsonObject();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("golden fixture is unreadable", e);
        }
    }
}