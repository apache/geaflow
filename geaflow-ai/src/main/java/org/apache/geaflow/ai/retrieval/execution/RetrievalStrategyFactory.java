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
package org.apache.geaflow.ai.retrieval.execution;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;

/**
 * Resolves the pluggable retrieval strategies named by configuration.
 *
 * <p>Each id is an explicit selection rather than a decorative label: an unknown id fails with a
 * typed error instead of silently falling back, so an operator who configures a strategy either
 * gets that strategy or is told it does not exist. New implementations register here, which keeps
 * the SPI extension point real without changing the service assembly.
 */
public final class RetrievalStrategyFactory {

    /** Configuration id of the default deterministic reranker. */
    public static final String DETERMINISTIC_RERANKER = "deterministic-reranker-v1";
    /** Configuration id of the default character-count token estimator. */
    public static final String CHARS4_TOKEN_ESTIMATOR = "chars4-v1";
    /** Configuration id of the default deterministic rule routing policy. */
    public static final String DETERMINISTIC_RULE_POLICY = "deterministic-rule";

    private RetrievalStrategyFactory() {
    }

    public static Reranker reranker(String id) {
        if (DETERMINISTIC_RERANKER.equals(normalize(id))) {
            return new DeterministicReranker();
        }
        throw unsupported("reranker", id);
    }

    public static TokenEstimator tokenEstimator(String id) {
        if (CHARS4_TOKEN_ESTIMATOR.equals(normalize(id))) {
            return new DeterministicTokenEstimator();
        }
        throw unsupported("token estimator", id);
    }

    public static RoutingPolicy routingPolicy(String id, RetrievalProperties properties) {
        if (DETERMINISTIC_RULE_POLICY.equals(normalize(id))) {
            return new DeterministicRulePolicy(properties.getHighAverageIdf(),
                properties.getHighScoreGap(), properties.getLongQueryLength(),
                properties.getStrongAnchorConfidence(), properties.getMinimumAnchorCount(),
                properties.getHighCandidateDegree(), properties.getHighGrowthRate(),
                properties.getBaseNeighborCap());
        }
        throw unsupported("routing policy", id);
    }

    private static String normalize(String id) {
        return id == null ? null : id.trim();
    }

    private static RetrievalException unsupported(String kind, String id) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST,
            "unsupported " + kind + " id: " + id);
    }
}