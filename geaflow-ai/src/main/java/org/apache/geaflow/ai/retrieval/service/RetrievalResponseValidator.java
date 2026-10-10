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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;

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
        required(trace.getRequestId(), "trace.requestId");
        if (!response.getRequestId().equals(trace.getRequestId())) {
            throw invalid("trace.requestId must match requestId");
        }
        required(trace.getOriginalQuery(), "originalQuery");
        if (trace.getSelectedMode() == null) {
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
        if (trace.getGraphVersion() != null
            && !response.getGraphVersion().equals(trace.getGraphVersion())) {
            throw invalid("trace.graphVersion must match graphVersion");
        }
        RetrievalBudget effectiveBudget = response.getEffectiveBudget();
        RetrievalBudgetValidator.validate(effectiveBudget, properties, true);
        if (trace.getSelectedMode().canonical() == RetrievalMode.HYBRID) {
            validateHybridTrace(trace, effectiveBudget);
        }
        return response;
    }

    private static void validateHybridTrace(RetrievalTrace trace, RetrievalBudget effectiveBudget) {
        required(trace.getGraphVersion(), "trace.graphVersion");
        required(trace.getIndexVersion(), "trace.indexVersion");
        required(trace.getStopReason(), "trace.stopReason");
        try {
            RecallStopReason.valueOf(trace.getStopReason());
        } catch (IllegalArgumentException error) {
            throw invalid("unsupported trace.stopReason: " + trace.getStopReason());
        }
        if (!Double.isFinite(trace.getBm25Weight()) || trace.getBm25Weight() < 0.0
            || !Double.isFinite(trace.getVectorWeight()) || trace.getVectorWeight() < 0.0
            || !Double.isFinite(trace.getGraphWeight()) || trace.getGraphWeight() < 0.0
            || trace.getBm25Weight() == 0.0 && trace.getVectorWeight() == 0.0
            && trace.getGraphWeight() == 0.0) {
            throw invalid("trace fusion weights must be finite, non-negative, and non-zero");
        }
        if (trace.getRrfRankConstant() < 1) {
            throw invalid("trace.rrfRankConstant must be positive");
        }
        Set<String> expected = new HashSet<>();
        expected.add("BM25");
        expected.add("VECTOR");
        if (trace.getSelectedChannels() == null) {
            throw invalid("HYBRID trace selectedChannels is required");
        }
        if (trace.getSelectedChannels().contains("GRAPH")) {
            expected.add("GRAPH");
        }
        if (trace.getSelectedChannels().size() != expected.size()
            || !expected.equals(new HashSet<>(trace.getSelectedChannels()))) {
            throw invalid("HYBRID trace must select BM25 and VECTOR, with optional GRAPH channel");
        }
        validateChannelMapKeys(trace.getChannelBudgets(), expected, "channelBudgets");
        validateChannelMapKeys(trace.getEvaluatedCounts(), expected, "evaluatedCounts");
        validateChannelMapKeys(trace.getCandidateCounts(), expected, "candidateCounts");
        validateChannelMapKeys(trace.getChannelStatuses(), expected, "channelStatuses");
        validateChannelMapKeys(trace.getChannelStopReasons(), expected, "channelStopReasons");
        int budgetSum = 0;
        int evaluatedSum = 0;
        Set<String> degradedChannels = new HashSet<>();
        for (String channel : expected) {
            Integer channelBudget = trace.getChannelBudgets().get(channel);
            Integer evaluated = trace.getEvaluatedCounts().get(channel);
            Integer candidates = trace.getCandidateCounts().get(channel);
            if (channelBudget == null || channelBudget < 1) {
                throw invalid("channel budget must be positive: " + channel);
            }
            if (evaluated == null || evaluated < 0 || evaluated > channelBudget) {
                throw invalid("invalid evaluated count: " + channel);
            }
            if (candidates == null || candidates < 0
                || candidates > ("GRAPH".equals(channel) ? channelBudget : evaluated)) {
                throw invalid("invalid candidate count: " + channel);
            }
            if (trace.getChannelStatuses().get(channel) == null
                || trace.getChannelStopReasons().get(channel) == null) {
                throw invalid("channel status and stop reason are required: " + channel);
            }
            budgetSum += channelBudget;
            evaluatedSum += evaluated;
            boolean degraded = trace.getChannelStatuses().get(channel) != RecallStageStatus.SUCCESS;
            if (degraded) {
                degradedChannels.add(channel);
            }
            if (degraded != trace.getDegradedChannels().contains(channel)) {
                throw invalid("degradedChannels does not match channel status: " + channel);
            }
            if (degraded && !hasText(trace.getDegradationReasons().get(channel))) {
                throw invalid("degradation reason is required: " + channel);
            }
        }
        if (trace.getTotalCandidatesEvaluated() != evaluatedSum) {
            throw invalid("totalCandidatesEvaluated does not match channel counts");
        }
        if (degradedChannels.size() != trace.getDegradedChannels().size()
            || !degradedChannels.equals(new HashSet<>(trace.getDegradedChannels()))) {
            throw invalid("degradedChannels must contain exactly degraded channels");
        }
        if (!degradedChannels.equals(trace.getDegradationReasons().keySet())) {
            throw invalid("degradationReasons must contain exactly degraded channels");
        }
        if (trace.getEffectiveCandidateBudget() != budgetSum
            || trace.getEffectiveCandidateBudget() != effectiveBudget.getMaxCandidates()
            || trace.getEffectiveTopK() != effectiveBudget.getTopK()) {
            throw invalid("invalid effective trace budget");
        }
    }

    private static void validateChannelMapKeys(Map<?, ?> values, Set<String> expected, String name) {
        if (values == null || !expected.equals(values.keySet())) {
            throw invalid(name + " must contain the selected Hybrid channels");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
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
