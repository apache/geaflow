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

package org.apache.geaflow.ai.retrieval.answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Optional fixed-rate concurrency run over Evidence captured from the HTTP load test. */
class AnswerConsumerStressTest {

    private static final Gson GSON = new Gson();
    private static final int MAX_FIXTURES = 256;

    @Test
    void consumesCapturedEvidenceAtFixedRate() throws Exception {
        String inputValue = System.getProperty("graphrag.answer.input");
        Assumptions.assumeTrue(inputValue != null && !inputValue.trim().isEmpty(),
            "set -Dgraphrag.answer.input to request-results.jsonl.gz to run the stress profile");
        Path input = Paths.get(inputValue);
        List<Fixture> fixtures = readFixtures(input);
        assertFalse(fixtures.isEmpty(), "load artifact must contain successful non-empty evidence");

        double qps = doubleProperty("graphrag.answer.qps", 50.0D);
        int durationSeconds = intProperty("graphrag.answer.durationSeconds", 120);
        int tokenBudget = intProperty("graphrag.answer.tokenBudget", 4096);
        int workers = Math.max(2, Math.min(32, Runtime.getRuntime().availableProcessors()));
        int scheduled = (int) Math.ceil(qps * durationSeconds);
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger invalid = new AtomicInteger();
        AtomicInteger abstained = new AtomicInteger();
        AtomicInteger citations = new AtomicInteger();
        AtomicInteger totalTokens = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencyNanos = new ConcurrentLinkedQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(workers, workers, 0L,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(workers * 4),
            new ThreadPoolExecutor.AbortPolicy());
        DefaultAnswerSynthesizer synthesizer =
            new DefaultAnswerSynthesizer(new DeterministicTokenEstimator());
        long started = System.nanoTime();

        for (int index = 0; index < scheduled; index++) {
            long target = started + (long) ((index / qps) * 1_000_000_000.0D);
            long delay = target - System.nanoTime();
            if (delay > 0L) {
                java.util.concurrent.locks.LockSupport.parkNanos(delay);
            }
            final Fixture fixture = fixtures.get(index % fixtures.size());
            try {
                executor.execute(() -> {
                    long requestStarted = System.nanoTime();
                    try {
                        AnswerResponse answer = synthesizer.synthesize(
                            new AnswerRequest(fixture.question, tokenBudget), fixture.evidence);
                        if (answer.getTokens() > tokenBudget) {
                            invalid.incrementAndGet();
                        }
                        Set<String> inputIds = new HashSet<>();
                        for (Evidence evidence : fixture.evidence) {
                            inputIds.add(evidence.getEvidenceId());
                        }
                        for (Citation citation : answer.getCitations()) {
                            if (!inputIds.contains(citation.getEvidenceId())) {
                                invalid.incrementAndGet();
                            }
                        }
                        if (answer.isAbstained()) {
                            abstained.incrementAndGet();
                            if (!answer.getCitations().isEmpty() || answer.getAnswer() != null) {
                                invalid.incrementAndGet();
                            }
                        } else if (answer.getCitations().isEmpty()) {
                            invalid.incrementAndGet();
                        }
                        citations.addAndGet(answer.getCitations().size());
                        totalTokens.addAndGet(answer.getTokens());
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                        invalid.incrementAndGet();
                    } finally {
                        latencyNanos.add(System.nanoTime() - requestStarted);
                        completed.incrementAndGet();
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException error) {
                rejected.incrementAndGet();
            }
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(Math.max(60, durationSeconds), TimeUnit.SECONDS),
            "consumer workers did not drain");

        List<Long> sorted = new ArrayList<>(latencyNanos);
        Collections.sort(sorted);
        assertEquals(0, rejected.get(), "fixed-rate producer must not drop work");
        assertEquals(scheduled, completed.get(), "every scheduled call must complete");
        assertEquals(0, invalid.get(), failure.get() == null ? "consumer result invariants" : failure.get().toString());
        System.out.println("answer-consumer-load " + GSON.toJson(new StressSummary(
            scheduled, completed.get(), rejected.get(), invalid.get(), abstained.get(),
            citations.get(), totalTokens.get(), percentileMs(sorted, 0.50D),
            percentileMs(sorted, 0.95D), percentileMs(sorted, 0.99D),
            (double) completed.get() / durationSeconds)));

        String outputValue = System.getProperty("graphrag.answer.output");
        if (outputValue != null && !outputValue.trim().isEmpty()) {
            Path output = Paths.get(outputValue);
            if (output.getParent() != null) {
                Files.createDirectories(output.getParent());
            }
            Files.write(output, GSON.toJson(new StressSummary(scheduled, completed.get(), rejected.get(),
                invalid.get(), abstained.get(), citations.get(), totalTokens.get(),
                percentileMs(sorted, 0.50D), percentileMs(sorted, 0.95D),
                percentileMs(sorted, 0.99D), (double) completed.get() / durationSeconds))
                .getBytes(StandardCharsets.UTF_8));
        }
    }

    private static List<Fixture> readFixtures(Path input) throws IOException {
        List<Fixture> fixtures = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new GZIPInputStream(Files.newInputStream(input)), StandardCharsets.UTF_8))) {
            String line;
            while (fixtures.size() < MAX_FIXTURES && (line = reader.readLine()) != null) {
                JsonObject row = new JsonParser().parse(line).getAsJsonObject();
                if (row.get("status").getAsInt() != 200 || !row.has("response")
                    || !row.get("response").isJsonObject()) {
                    continue;
                }
                RetrievalResponse response = RetrievalApiJson.parseResponse(
                    row.getAsJsonObject("response").toString());
                if (response.getEvidence().isEmpty()) {
                    continue;
                }
                String question = response.getTrace().getOriginalQuery();
                fixtures.add(new Fixture(question == null ? "question" : question,
                    response.getEvidence()));
            }
        }
        return fixtures;
    }

    private static int intProperty(String name, int fallback) {
        return Integer.parseInt(System.getProperty(name, String.valueOf(fallback)));
    }

    private static double doubleProperty(String name, double fallback) {
        return Double.parseDouble(System.getProperty(name, String.valueOf(fallback)));
    }

    private static double percentileMs(List<Long> ordered, double quantile) {
        if (ordered.isEmpty()) {
            return 0.0D;
        }
        int index = (int) Math.ceil(quantile * ordered.size()) - 1;
        return ordered.get(Math.max(0, Math.min(index, ordered.size() - 1))) / 1_000_000.0D;
    }

    private static final class Fixture {
        private final String question;
        private final List<Evidence> evidence;

        private Fixture(String question, List<Evidence> evidence) {
            this.question = question;
            this.evidence = evidence;
        }
    }

    private static final class StressSummary {
        private final int scheduled;
        private final int completed;
        private final int rejected;
        private final int invalid;
        private final int abstained;
        private final int citations;
        private final int totalTokens;
        private final double p50LatencyMs;
        private final double p95LatencyMs;
        private final double p99LatencyMs;
        private final double actualQps;

        private StressSummary(int scheduled, int completed, int rejected, int invalid,
                              int abstained, int citations, int totalTokens, double p50LatencyMs,
                              double p95LatencyMs, double p99LatencyMs, double actualQps) {
            this.scheduled = scheduled;
            this.completed = completed;
            this.rejected = rejected;
            this.invalid = invalid;
            this.abstained = abstained;
            this.citations = citations;
            this.totalTokens = totalTokens;
            this.p50LatencyMs = p50LatencyMs;
            this.p95LatencyMs = p95LatencyMs;
            this.p99LatencyMs = p99LatencyMs;
            this.actualQps = actualQps;
        }
    }
}
