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

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;

/** Validates the complete success response before it crosses the API boundary. */
public final class RetrievalResponseValidator {

    private RetrievalResponseValidator() {
    }

    public static RetrievalResponse validate(RetrievalResponse response,
                                              RetrievalProperties properties) {
        if (response == null) {
            throw invalid("response is required");
        }
        required(response.getRequestId(), "requestId");
        required(response.getGraphName(), "graphName");
        required(response.getGraphVersion(), "graphVersion");
        RetrievalTrace trace = response.getTrace();
        if (trace == null) {
            throw invalid("trace is required");
        }
        required(trace.getTraceVersion(), "traceVersion");
        required(trace.getOriginalQuery(), "originalQuery");
        if (trace.getSelectedMode() != RetrievalMode.KEYWORD) {
            throw unsupported("unsupported retrieval mode in trace");
        }
        if (trace.getExecutionMode() != ExecutionMode.SEQUENTIAL) {
            throw unsupported("unsupported execution mode in trace");
        }
        List<TraceStage> stages = trace.getStages();
        if (stages == null) {
            throw invalid("stages is required");
        }
        for (TraceStage stage : stages) {
            if (stage == null) {
                throw invalid("stages must not contain null");
            }
            required(stage.getName(), "stage.name");
            required(stage.getStatus(), "stage.status");
        }
        if (response.getEvidence() == null || response.getPaths() == null
            || response.getSources() == null || response.getDegradedChannels() == null) {
            throw invalid("response collections are required");
        }
        RetrievalBudget effectiveBudget = response.getEffectiveBudget();
        RetrievalBudgetValidator.validate(effectiveBudget, properties, true);
        return response;
    }

    public static RetrievalResponse normalizeCollections(RetrievalResponse response) {
        response.setEvidence(response.getEvidence() == null
            ? Collections.emptyList() : response.getEvidence());
        response.setPaths(response.getPaths() == null
            ? Collections.emptyList() : response.getPaths());
        response.setSources(response.getSources() == null
            ? Collections.emptyList() : response.getSources());
        response.setDegradedChannels(response.getDegradedChannels() == null
            ? Collections.emptyList() : response.getDegradedChannels());
        return response;
    }

    private static void required(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw invalid(name + " is required");
        }
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, message);
    }

    private static RetrievalException unsupported(String message) {
        return new RetrievalException(RetrievalErrorCode.UNSUPPORTED_OPTION, message);
    }
}
