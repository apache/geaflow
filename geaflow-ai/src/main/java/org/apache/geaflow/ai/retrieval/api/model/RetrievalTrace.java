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

package org.apache.geaflow.ai.retrieval.api.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;

/** Stable minimum trace shape; future fields must be additive. */
public class RetrievalTrace {

    private String traceVersion;
    private String requestId;
    private String originalQuery;
    private RetrievalMode selectedMode;
    private ExecutionMode executionMode;
    private List<TraceStage> stages = new ArrayList<>();
    private String stopReason;
    private String graphVersion;
    private String indexVersion;
    private String vectorVersion;
    private int effectiveCandidateBudget;
    private long elapsedNanos;
    private Map<String, Integer> candidateCounts = new LinkedHashMap<>();
    private int anchorsConsidered;
    private int edgesExamined;
    private int neighborsSampled;
    private int verticesReached;
    private int graphCandidatesProduced;
    private List<String> degradedChannels = new ArrayList<>();
    private List<String> validationErrors = new ArrayList<>();
    private List<String> selectedChannels = new ArrayList<>();
    private int totalCandidatesEvaluated;
    private Map<String, Integer> channelBudgets = new LinkedHashMap<>();
    private Map<String, String> degradationReasons = new LinkedHashMap<>();
    private double bm25Weight = 1.0;
    private double vectorWeight = 1.0;
    private double graphWeight = 1.0;
    private int rrfRankConstant = 60;
    private int effectiveTopK;
    private Map<String, Integer> evaluatedCounts = new LinkedHashMap<>();
    private Map<String, RecallStageStatus> channelStatuses = new LinkedHashMap<>();
    private Map<String, RecallStopReason> channelStopReasons = new LinkedHashMap<>();

    public RetrievalTrace() {
    }

    public String getTraceVersion() {
        return traceVersion;
    }

    public void setTraceVersion(String traceVersion) {
        this.traceVersion = traceVersion;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public RetrievalMode getSelectedMode() {
        return selectedMode;
    }

    public void setSelectedMode(RetrievalMode selectedMode) {
        this.selectedMode = selectedMode;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(ExecutionMode executionMode) {
        this.executionMode = executionMode;
    }

    public List<TraceStage> getStages() {
        return stages;
    }

    public void setStages(List<TraceStage> stages) {
        this.stages = stages == null ? new ArrayList<TraceStage>() : new ArrayList<>(stages);
    }

    public String getStopReason() {
        return stopReason;
    }

    public void setStopReason(String stopReason) {
        this.stopReason = stopReason;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public void setGraphVersion(String graphVersion) {
        this.graphVersion = graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public void setIndexVersion(String indexVersion) {
        this.indexVersion = indexVersion;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public void setVectorVersion(String vectorVersion) {
        this.vectorVersion = vectorVersion;
    }

    public int getEffectiveCandidateBudget() {
        return effectiveCandidateBudget;
    }

    public void setEffectiveCandidateBudget(int value) {
        this.effectiveCandidateBudget = value;
    }

    public long getElapsedNanos() {
        return elapsedNanos;
    }

    public void setElapsedNanos(long value) {
        this.elapsedNanos = value;
    }

    public Map<String, Integer> getCandidateCounts() {
        return candidateCounts;
    }

    public void setCandidateCounts(Map<String, Integer> value) {
        candidateCounts = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }

    public int getAnchorsConsidered() {
        return anchorsConsidered;
    }

    public void setAnchorsConsidered(int value) {
        anchorsConsidered = value;
    }

    public int getEdgesExamined() {
        return edgesExamined;
    }

    public void setEdgesExamined(int value) {
        edgesExamined = value;
    }

    public int getNeighborsSampled() {
        return neighborsSampled;
    }

    public void setNeighborsSampled(int value) {
        neighborsSampled = value;
    }

    public int getVerticesReached() {
        return verticesReached;
    }

    public void setVerticesReached(int value) {
        verticesReached = value;
    }

    public int getGraphCandidatesProduced() {
        return graphCandidatesProduced;
    }

    public void setGraphCandidatesProduced(int value) {
        graphCandidatesProduced = value;
    }

    public List<String> getDegradedChannels() {
        return degradedChannels;
    }

    public void setDegradedChannels(List<String> value) {
        degradedChannels = value == null ? new ArrayList<String>() : new ArrayList<>(value);
    }

    public List<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(List<String> value) {
        validationErrors = value == null ? new ArrayList<String>() : new ArrayList<>(value);
    }

    public List<String> getSelectedChannels() {
        return selectedChannels;
    }

    public void setSelectedChannels(List<String> value) {
        selectedChannels = value == null ? new ArrayList<String>() : new ArrayList<>(value);
    }

    public int getTotalCandidatesEvaluated() {
        return totalCandidatesEvaluated;
    }

    public void setTotalCandidatesEvaluated(int value) {
        totalCandidatesEvaluated = value;
    }

    public Map<String, Integer> getChannelBudgets() {
        return channelBudgets;
    }

    public void setChannelBudgets(Map<String, Integer> value) {
        channelBudgets = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }

    public Map<String, String> getDegradationReasons() {
        return degradationReasons;
    }

    public void setDegradationReasons(Map<String, String> value) {
        degradationReasons = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }

    public double getBm25Weight() {
        return bm25Weight;
    }

    public void setBm25Weight(double value) {
        bm25Weight = value;
    }

    public double getVectorWeight() {
        return vectorWeight;
    }

    public void setVectorWeight(double value) {
        vectorWeight = value;
    }

    public double getGraphWeight() {
        return graphWeight;
    }

    public void setGraphWeight(double value) {
        graphWeight = value;
    }

    public int getRrfRankConstant() {
        return rrfRankConstant;
    }

    public void setRrfRankConstant(int value) {
        rrfRankConstant = value;
    }

    public int getEffectiveTopK() {
        return effectiveTopK;
    }

    public void setEffectiveTopK(int value) {
        effectiveTopK = value;
    }

    public Map<String, Integer> getEvaluatedCounts() {
        return evaluatedCounts;
    }

    public void setEvaluatedCounts(Map<String, Integer> value) {
        evaluatedCounts = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }

    public Map<String, RecallStageStatus> getChannelStatuses() {
        return channelStatuses;
    }

    public void setChannelStatuses(Map<String, RecallStageStatus> value) {
        channelStatuses = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }

    public Map<String, RecallStopReason> getChannelStopReasons() {
        return channelStopReasons;
    }

    public void setChannelStopReasons(Map<String, RecallStopReason> value) {
        channelStopReasons = value == null ? new LinkedHashMap<>() : new LinkedHashMap<>(value);
    }
}
