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

package org.apache.geaflow.ai.retrieval.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.junit.jupiter.api.Test;

class ExecutionBudgetTest {

    @Test
    void ledgerChargesCandidatesAndUniqueEvidenceOnlyOnce() {
        BudgetLedger ledger = new BudgetLedger(2, 3, 0L);
        TextChunk chunk = chunk("c1", 2);

        assertTrue(ledger.tryEvaluateCandidate());
        assertTrue(ledger.tryEvaluateCandidate());
        assertFalse(ledger.tryEvaluateCandidate());
        assertTrue(ledger.tryRetain("c1", chunk, new DeterministicTokenEstimator()));
        assertTrue(ledger.tryRetain("c1", chunk, new DeterministicTokenEstimator()));
        assertEquals(2, ledger.getTokensRetained());
        assertEquals(RecallStopReason.CANDIDATE_BUDGET, ledger.getStopReason());
    }

    @Test
    void tokenTruncationSkipsOversizedEvidenceDeterministically() {
        TextChunk first = chunk("c1", 3);
        TextChunk second = chunk("c2", 1);
        Evidence oversized = evidence("a", first, 0.9);
        Evidence retained = evidence("b", second, 0.8);

        TokenContextTruncator.Result result = new TokenContextTruncator(new DeterministicTokenEstimator())
            .truncate(Arrays.asList(oversized, retained), 2, 2);

        assertEquals(Collections.singletonList(retained), result.getEvidence());
        assertEquals(1, result.getTokensRetained());
        assertEquals(RecallStopReason.TOKEN_BUDGET, result.getStopReason());
    }

    @Test
    void tokenTruncationReportsSkippedEvidenceEvenWhenTopKIsFilled() {
        Evidence oversized = evidence("a", chunk("c1", 3), 0.9);
        Evidence firstSmall = evidence("b", chunk("c2", 1), 0.8);
        Evidence secondSmall = evidence("c", chunk("c3", 1), 0.7);

        TokenContextTruncator.Result result = new TokenContextTruncator(new DeterministicTokenEstimator())
            .truncate(Arrays.asList(oversized, firstSmall, secondSmall), 2, 2);

        assertEquals(Arrays.asList(firstSmall, secondSmall), result.getEvidence());
        assertEquals(2, result.getTokensRetained());
        assertEquals(RecallStopReason.TOKEN_BUDGET, result.getStopReason());
    }

    @Test
    void parallelStagesKeepFailuresIsolatedAndCollectByStableName() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(2);
        try {
            StageExecutor.Stage<String> success = new StageExecutor.Stage<>("BM25", () -> {
                started.countDown();
                assertTrue(started.await(1, TimeUnit.SECONDS));
                return "ok";
            });
            StageExecutor.Stage<String> failure = new StageExecutor.Stage<>("GRAPH", () -> {
                started.countDown();
                assertTrue(started.await(1, TimeUnit.SECONDS));
                throw new IllegalStateException("broken stage");
            });

            Map<String, StageExecutor.StageResult<String>> results = new StageExecutor()
                .execute(Arrays.asList(failure, success), ExecutionMode.PARALLEL, executor);

            assertEquals(Arrays.asList("BM25", "GRAPH"), new ArrayList<>(results.keySet()));
            assertTrue(results.get("BM25").isSuccess());
            assertEquals("ok", results.get("BM25").getValue());
            assertFalse(results.get("GRAPH").isSuccess());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void parallelStagesCancelWorkAtTheSharedDeadline() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        try {
            StageExecutor.Stage<String> blocked = new StageExecutor.Stage<>("BM25", () -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                    return "late";
                } finally {
                    stopped.countDown();
                }
            });
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);

            Map<String, StageExecutor.StageResult<String>> results = new StageExecutor()
                .execute(Collections.singletonList(blocked), ExecutionMode.PARALLEL, executor,
                    deadline, System::nanoTime);

            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(results.get("BM25").isSuccess());
            assertTrue(results.get("BM25").getError() instanceof TimeoutException);
            assertTrue(stopped.await(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void parallelDeadlineReturnsWhenTaskIgnoresInterruption() throws Exception {
        ExecutorService stageExecutor = Executors.newSingleThreadExecutor();
        ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        try {
            StageExecutor.Stage<String> blocked = new StageExecutor.Stage<>("BM25", () -> {
                started.countDown();
                while (release.getCount() > 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // Simulate a provider that cannot abort its current I/O operation.
                    }
                }
                stopped.countDown();
                return "late";
            });
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(80);
            Future<Map<String, StageExecutor.StageResult<String>>> result = callerExecutor.submit(
                () -> new StageExecutor().execute(Collections.singletonList(blocked),
                    ExecutionMode.PARALLEL, stageExecutor, deadline, System::nanoTime));

            assertTrue(started.await(1, TimeUnit.SECONDS));
            Map<String, StageExecutor.StageResult<String>> results = result.get(1, TimeUnit.SECONDS);

            assertFalse(results.get("BM25").isSuccess());
            assertTrue(results.get("BM25").getError() instanceof TimeoutException);
            assertEquals(1L, stopped.getCount());
        } finally {
            release.countDown();
            assertTrue(stopped.await(1, TimeUnit.SECONDS));
            stageExecutor.shutdownNow();
            callerExecutor.shutdownNow();
        }
    }

    @Test
    void sequentialStagesCheckTheSharedDeadlineBeforeAndAfterExecution() {
        AtomicLong time = new AtomicLong(1L);
        StageExecutor.Stage<String> first = new StageExecutor.Stage<>("BM25", () -> {
            time.set(10L);
            return "completed";
        });
        StageExecutor.Stage<String> second = new StageExecutor.Stage<>("VECTOR", () -> "not-run");

        Map<String, StageExecutor.StageResult<String>> results = new StageExecutor()
            .execute(Arrays.asList(first, second), ExecutionMode.SEQUENTIAL, null, 10L, time::get);

        assertTrue(results.get("BM25").isSuccess());
        assertEquals("completed", results.get("BM25").getValue());
        assertFalse(results.get("VECTOR").isSuccess());
        assertTrue(results.get("VECTOR").getError() instanceof TimeoutException);
    }

    private static TextChunk chunk(String id, int tokens) {
        return new TextChunk(id, "doc-" + id, 0, 0, 1, tokens, "text");
    }

    private static Evidence evidence(String id, TextChunk chunk, double score) {
        return new Evidence(id, EvidenceKind.CHUNK, chunk.getText(), Collections.singletonList(chunk),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(),
            score, 1);
    }
}
