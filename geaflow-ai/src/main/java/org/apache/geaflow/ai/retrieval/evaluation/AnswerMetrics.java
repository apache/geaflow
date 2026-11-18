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

import java.util.Locale;

/** Deterministic extractive answer normalization and overlap metrics. */
public final class AnswerMetrics {
    private AnswerMetrics() {
    }

    public static String normalize(String answer) {
        if (answer == null) {
            return "";
        }
        return answer.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N} ]", " ")
            .replaceAll("\\s+", " ").trim();
    }

    public static boolean exactMatch(String prediction, String gold) {
        return normalize(prediction).equals(normalize(gold));
    }

    public static double f1(String prediction, String gold) {
        String[] predicted = tokens(prediction);
        String[] expected = tokens(gold);
        if (predicted.length == 0 || expected.length == 0) {
            return predicted.length == expected.length ? 1.0D : 0.0D;
        }
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        for (String token : expected) {
            counts.put(token, counts.getOrDefault(token, 0) + 1);
        }
        int overlap = 0;
        for (String token : predicted) {
            Integer count = counts.get(token);
            if (count != null && count > 0) {
                overlap++;
                counts.put(token, count - 1);
            }
        }
        if (overlap == 0) {
            return 0.0D;
        }
        double precision = overlap / (double) predicted.length;
        double recall = overlap / (double) expected.length;
        return 2.0D * precision * recall / (precision + recall);
    }

    private static String[] tokens(String answer) {
        String normalized = normalize(answer);
        return normalized.isEmpty() ? new String[0] : normalized.split(" ");
    }
}
