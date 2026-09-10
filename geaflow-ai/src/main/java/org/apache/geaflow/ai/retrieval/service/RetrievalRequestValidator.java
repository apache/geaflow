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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.service;

import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;

/** Converts an untrusted wire request into a bounded immutable command. */
public class RetrievalRequestValidator {

    private static final int MAX_GRAPH_NAME_LENGTH = 128;
    private static final int MAX_QUERY_LENGTH = 4096;
    private final RetrievalProperties properties;

    public RetrievalRequestValidator(RetrievalProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.properties.validateConfiguration();
    }

    public RetrievalCommand validate(RetrievalRequest request) {
        if (request == null) {
            throw invalid("request is required");
        }
        final String graphName = trimmed(request.getGraphName(), "graphName", MAX_GRAPH_NAME_LENGTH);
        final String query = trimmed(request.getQuery(), "query", MAX_QUERY_LENGTH);
        final RetrievalMode mode = parseMode(request.getMode() == null
            ? properties.getDefaultMode() : request.getMode());
        final ExecutionMode executionMode = parseExecutionMode(request.getExecutionMode() == null
            ? properties.getDefaultExecutionMode() : request.getExecutionMode());
        List<Double> vector = request.getQueryVector();
        if (vector != null && !vector.isEmpty()) {
            throw unsupported("non-empty queryVector is not supported in v1");
        }

        RetrievalBudget input = request.getBudget();
        final RetrievalBudget effectiveBudget = RetrievalBudgetValidator.validate(input,
            properties, false);
        return new RetrievalCommand(graphName, query, mode, executionMode,
            effectiveBudget);
    }

    private static String trimmed(String value, String name, int maxLength) {
        if (value == null || value.trim().isEmpty()) {
            throw invalid(name + " is required");
        }
        String result = value.trim();
        if (result.length() > maxLength) {
            throw invalid(name + " exceeds maximum length " + maxLength);
        }
        return result;
    }

    private static RetrievalMode parseMode(String value) {
        if (value == null || !RetrievalMode.KEYWORD.name().equals(value)) {
            throw unsupported("unsupported retrieval mode: " + value);
        }
        return RetrievalMode.KEYWORD;
    }

    private static ExecutionMode parseExecutionMode(String value) {
        if (value == null) {
            throw unsupported("unsupported execution mode: null");
        }
        try {
            ExecutionMode mode = ExecutionMode.valueOf(value);
            if (mode != ExecutionMode.SEQUENTIAL) {
                throw unsupported("unsupported execution mode: " + value);
            }
            return mode;
        } catch (IllegalArgumentException e) {
            throw unsupported("unsupported execution mode: " + value);
        }
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, message);
    }

    private static RetrievalException unsupported(String message) {
        return new RetrievalException(RetrievalErrorCode.UNSUPPORTED_OPTION, message);
    }
}
