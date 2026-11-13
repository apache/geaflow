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

package org.apache.geaflow.ai.retrieval.service;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;

/** Applies the same budget invariants at every API boundary. */
public final class RetrievalBudgetValidator {

    private RetrievalBudgetValidator() {
    }

    public static RetrievalBudget validate(RetrievalBudget budget, RetrievalProperties properties,
                                           boolean requireAllValues) {
        if (budget == null) {
            if (requireAllValues) {
                throw invalid("budget is required");
            }
            return defaults(properties);
        }
        Integer topK = valueOrDefault(budget.getTopK(), properties.getDefaultTopK());
        Integer timeoutMs = valueOrDefault(budget.getTimeoutMs(), properties.getDefaultTimeoutMs());
        Integer maxCandidates = valueOrDefault(budget.getMaxCandidates(),
            properties.getDefaultMaxCandidates());
        final Integer tokenBudget = valueOrDefault(budget.getTokenBudget(),
            properties.getDefaultTokenBudget());
        range("topK", topK, properties.getMaxTopK());
        range("timeoutMs", timeoutMs, properties.getMaxTimeoutMs());
        range("maxCandidates", maxCandidates, properties.getMaxCandidates());
        range("tokenBudget", tokenBudget, properties.getMaxTokenBudget());
        if (topK > maxCandidates) {
            throw invalid("topK must not exceed maxCandidates");
        }
        return new RetrievalBudget(topK, timeoutMs, maxCandidates, tokenBudget);
    }

    private static RetrievalBudget defaults(RetrievalProperties properties) {
        return new RetrievalBudget(properties.getDefaultTopK(), properties.getDefaultTimeoutMs(),
            properties.getDefaultMaxCandidates(), properties.getDefaultTokenBudget());
    }

    private static int valueOrDefault(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static void range(String name, int value, int max) {
        if (value < 1 || value > max) {
            throw invalid(name + " must be between 1 and " + max);
        }
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, message);
    }
}
