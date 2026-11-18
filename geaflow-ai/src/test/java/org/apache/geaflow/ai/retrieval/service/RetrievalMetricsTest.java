/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.junit.jupiter.api.Test;

class RetrievalMetricsTest {

    @Test
    void aggregatesWorkAndReasonsFromResponseTrace() {
        RetrievalMetrics metrics = new RetrievalMetrics();
        RetrievalTrace trace = new RetrievalTrace();
        trace.setSelectedChannels(Collections.singletonList("GRAPH"));
        trace.setEvaluatedCounts(Collections.singletonMap("GRAPH", 4));
        trace.setStageElapsedNanos(Collections.singletonMap("GRAPH", 100L));
        trace.setEdgesExamined(3);
        trace.setTokensRetained(12);
        trace.setDegradationReasons(Collections.singletonMap("VECTOR", "INDEX_MISSING"));
        RetrievalResponse response = new RetrievalResponse();
        response.setTrace(trace);

        metrics.recordResponse(response);

        RetrievalMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(1L, snapshot.getChannelCounts().get("GRAPH"));
        assertEquals(4L, snapshot.getCandidateCounts().get("GRAPH"));
        assertEquals(100L, snapshot.getStageElapsedNanos().get("GRAPH"));
        assertEquals(3L, snapshot.getEdgesExamined());
        assertEquals(12L, snapshot.getTokensRetained());
        assertEquals(1L, snapshot.getDegradationReasons().get("VECTOR:INDEX_MISSING"));
    }
}
