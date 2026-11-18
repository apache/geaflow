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
package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.DeterministicReranker;
import org.apache.geaflow.ai.retrieval.execution.DeterministicRulePolicy;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.execution.Reranker;
import org.apache.geaflow.ai.retrieval.execution.RetrievalStrategyFactory;
import org.apache.geaflow.ai.retrieval.execution.AdaptivePolicy;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the strategy ids in configuration actually select an implementation.
 *
 * <p>These ids previously acted as a whitelist that validated a literal and was then ignored: the
 * service constructed the deterministic implementations directly. That makes the SPI look
 * pluggable while offering no way to switch it, and it hides typos that should be typed errors.
 */
class StrategySelectionTest {

    @Test
    void knownIdsResolveToTheirImplementation() {
        assertTrue(RetrievalStrategyFactory.reranker("deterministic-reranker-v1")
            instanceof DeterministicReranker);
        assertTrue(RetrievalStrategyFactory.tokenEstimator("chars4-v1")
            instanceof DeterministicTokenEstimator);
        assertTrue(RetrievalStrategyFactory.adaptivePolicy("deterministic-rule", new RetrievalProperties())
            instanceof DeterministicRulePolicy);
        assertEquals("chars4-v1", RetrievalStrategyFactory.tokenEstimator(" chars4-v1 ").getVersion(),
            "surrounding whitespace must not defeat selection");
    }

    @Test
    void unknownIdsFailWithTypedErrorsInsteadOfSilentlyFallingBack() {
        assertCode(RetrievalErrorCode.INVALID_REQUEST,
            () -> RetrievalStrategyFactory.reranker("unknown-reranker"));
        assertCode(RetrievalErrorCode.INVALID_REQUEST,
            () -> RetrievalStrategyFactory.tokenEstimator("unknown-estimator"));
        assertCode(RetrievalErrorCode.INVALID_REQUEST,
            () -> RetrievalStrategyFactory.adaptivePolicy("unknown-policy", new RetrievalProperties()));
        assertCode(RetrievalErrorCode.INVALID_REQUEST, () -> RetrievalStrategyFactory.reranker(null));
    }

    @Test
    void factoryResolvedStrategiesExposeTheirVersions() {
        Reranker reranker = RetrievalStrategyFactory.reranker("deterministic-reranker-v1");
        TokenEstimator estimator = RetrievalStrategyFactory.tokenEstimator("chars4-v1");
        AdaptivePolicy policy = RetrievalStrategyFactory.adaptivePolicy("deterministic-rule",
            new RetrievalProperties());

        assertEquals("deterministic-reranker-v1", reranker.getVersion());
        assertEquals("chars4-v1", estimator.getVersion());
        assertEquals("deterministic-rule", policy.getPolicyId());
    }

    private static void assertCode(RetrievalErrorCode code,
                                   org.junit.jupiter.api.function.Executable executable) {
        RetrievalException exception = assertThrows(RetrievalException.class, executable);
        assertEquals(code, exception.getCode());
    }
}
