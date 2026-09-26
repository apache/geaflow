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
import java.util.UUID;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.operator.GraphSearchStore.ScoredGraphEntity;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.service.ServerMemoryCache;
import org.noear.solon.annotation.Component;
import org.noear.solon.annotation.Init;
import org.noear.solon.annotation.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes the bounded keyword retrieval flow without creating a session. */
@Component
public class RetrievalService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetrievalService.class);

    @Inject
    private ServerMemoryCache cache;
    @Inject
    private RetrievalProperties properties;
    @Inject
    private RetrievalMetrics metrics;
    private RetrievalRequestValidator validator;
    private final EvidenceMapper evidenceMapper = new EvidenceMapper();

    public RetrievalService() {
        this(new ServerMemoryCache(), new RetrievalProperties(), new RetrievalMetrics());
    }

    public RetrievalService(ServerMemoryCache cache, RetrievalProperties properties) {
        this(cache, properties, new RetrievalMetrics());
    }

    public RetrievalService(ServerMemoryCache cache, RetrievalProperties properties,
                            RetrievalMetrics metrics) {
        this.cache = cache;
        this.properties = properties;
        this.metrics = metrics;
        this.properties.validateConfiguration();
        this.validator = new RetrievalRequestValidator(properties);
    }

    @Init
    public void initialize() {
        properties.validateConfiguration();
        validator = new RetrievalRequestValidator(properties);
    }

    public RetrievalResponse retrieve(RetrievalRequest request) {
        return retrieve(request, UUID.randomUUID().toString());
    }

    public RetrievalResponse retrieve(RetrievalRequest request, String requestId) {
        long startedAt = System.nanoTime();
        try {
            RetrievalCommand command = validator.validate(request);
            ServerMemoryCache.ReadyStatus readiness = cache.keywordReadiness(command.getGraphName());
            if ("GRAPH_NOT_LOADED".equals(readiness.getReason())) {
                throw new RetrievalException(RetrievalErrorCode.GRAPH_NOT_FOUND,
                    "graph not found: " + command.getGraphName());
            }
            if (!readiness.isReady()) {
                throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                    "keyword retrieval is not ready: " + readiness.getReason());
            }
            GraphMemoryServer server = cache.getServerByName(command.getGraphName());
            if (server == null) {
                throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                    "keyword retrieval is not ready: SERVER_NOT_LOADED");
            }
            RetrievalBudget budget = command.getBudget();
            long deadline = System.nanoTime() + budget.getTimeoutMs() * 1_000_000L;
            final List<ScoredGraphEntity> hits = server.searchKeyword(command.getQuery(),
                budget.getTopK(), budget.getMaxCandidates(), deadline);
            if (System.nanoTime() > deadline) {
                throw new RetrievalException(RetrievalErrorCode.RETRIEVAL_TIMEOUT,
                    "retrieval deadline exceeded");
            }
            RetrievalResponse response = new RetrievalResponse();
            response.setRequestId(requestId);
            response.setGraphName(command.getGraphName());
            response.setGraphVersion(cache.getGraphVersion(command.getGraphName()));
            response.setEvidence(evidenceMapper.map(hits));
            response.setPaths(Collections.emptyList());
            response.setSources(Collections.emptyList());
            response.setDegradedChannels(Collections.emptyList());
            response.setEffectiveBudget(budget);
            RetrievalTrace trace = new RetrievalTrace();
            trace.setTraceVersion("v1");
            trace.setOriginalQuery(command.getQuery());
            trace.setSelectedMode(command.getMode());
            trace.setExecutionMode(command.getExecutionMode());
            trace.setStages(Collections.singletonList(new TraceStage("keyword", "SUCCESS", null)));
            trace.setStopReason(hits.isEmpty() ? "NO_MATCH" : "TOP_K");
            response.setTrace(trace);
            long elapsedMs = elapsedMs(startedAt);
            metrics.recordSuccess(elapsedMs);
            LOGGER.info("retrieval completed graph={} mode={} topK={} maxCandidates={} "
                    + "timeoutMs={} candidateBudget={} results={} status=SUCCESS elapsedMs={}",
                command.getGraphName(), command.getMode(), budget.getTopK(),
                budget.getMaxCandidates(), budget.getTimeoutMs(), budget.getMaxCandidates(),
                hits.size(), elapsedMs);
            return response;
        } catch (RetrievalException exception) {
            long elapsedMs = elapsedMs(startedAt);
            metrics.recordFailure(exception.getCode(), elapsedMs);
            LOGGER.info("retrieval completed status={} code={} elapsedMs={}",
                "FAILURE", exception.getCode(), elapsedMs);
            throw exception;
        } catch (RuntimeException exception) {
            long elapsedMs = elapsedMs(startedAt);
            metrics.recordFailure(RetrievalErrorCode.INTERNAL_ERROR, elapsedMs);
            LOGGER.warn("retrieval failed status=FAILURE code={} elapsedMs={}",
                RetrievalErrorCode.INTERNAL_ERROR, elapsedMs, exception);
            throw new RetrievalException(RetrievalErrorCode.INTERNAL_ERROR,
                "internal retrieval error");
        }
    }

    private static long elapsedMs(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }
}
