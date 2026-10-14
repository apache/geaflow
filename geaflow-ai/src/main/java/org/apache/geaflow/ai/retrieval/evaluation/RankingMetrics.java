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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Ranking and path metrics over stable evidence identifiers. */
public final class RankingMetrics {
    private RankingMetrics() {
    }

    public static double recallAtK(List<String> ranked, Set<String> relevant, int k) {
        if (relevant == null || relevant.isEmpty()) {
            return 0.0D;
        }
        Set<String> found = new HashSet<>();
        for (int i = 0; i < Math.min(k, ranked.size()); i++) {
            if (relevant.contains(ranked.get(i))) {
                found.add(ranked.get(i));
            }
        }
        return found.size() / (double) relevant.size();
    }

    public static double reciprocalRank(List<String> ranked, Set<String> relevant) {
        for (int i = 0; i < ranked.size(); i++) {
            if (relevant.contains(ranked.get(i))) {
                return 1.0D / (i + 1.0D);
            }
        }
        return 0.0D;
    }

    public static double ndcgAtK(List<String> ranked, Set<String> relevant, int k) {
        if (relevant == null || relevant.isEmpty()) {
            return 0.0D;
        }
        double dcg = 0.0D;
        for (int i = 0; i < Math.min(k, ranked.size()); i++) {
            if (relevant.contains(ranked.get(i))) {
                dcg += 1.0D / (Math.log(i + 2.0D) / Math.log(2.0D));
            }
        }
        double ideal = 0.0D;
        for (int i = 0; i < Math.min(k, relevant.size()); i++) {
            ideal += 1.0D / (Math.log(i + 2.0D) / Math.log(2.0D));
        }
        return ideal == 0.0D ? 0.0D : dcg / ideal;
    }

    public static double relationPathHit(Set<String> predicted, Set<String> targets) {
        if (targets == null || targets.isEmpty()) {
            return 0.0D;
        }
        for (String target : targets) {
            if (predicted.contains(target)) {
                return 1.0D;
            }
        }
        return 0.0D;
    }
}
