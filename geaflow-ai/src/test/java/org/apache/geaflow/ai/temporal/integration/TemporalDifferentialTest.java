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

package org.apache.geaflow.ai.temporal.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Small legal event streams generated without consulting either replay implementation. */
public class TemporalDifferentialTest {

    private static final long[] SEEDS = {261450009L, 261450010L, 261450011L, 261450012L};
    private static final int KEY_COUNT = 6;
    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testFixedSeedsAcrossBatchSizesAndLegalArrivalOrders() {
        for (long seed : SEEDS) {
            List<List<NormalizedMemoryEvent>> timelines = timelines(seed);
            List<NormalizedMemoryEvent> sorted = new ArrayList<>();
            for (List<NormalizedMemoryEvent> timeline : timelines) {
                sorted.addAll(timeline);
            }
            Collections.sort(sorted, EVENT_ORDER);

            List<NormalizedMemoryEvent> shuffled = new ArrayList<>(sorted);
            Collections.shuffle(shuffled, new Random(seed));
            IncrementalTemporalIntegrator allAtOnce = new IncrementalTemporalIntegrator();
            String fullContext = context(seed, "shuffled-full", 0, shuffled);
            verify(seed, "shuffled-full", 0, shuffled, allAtOnce, () -> {
                allAtOnce.applyBatch(shuffled);
                Assertions.assertEquals(sorted, allAtOnce.normalizedEventSnapshot(), fullContext);
                assertTemplateOutcome(allAtOnce.stateSnapshot(), fullContext);
            });

            for (int batchSize : new int[] {2, 7, 13}) {
                verifySortedBatches(seed, sorted, batchSize, allAtOnce.stateSnapshot());
            }
            verifyCrossKeyArrival(seed, timelines, allAtOnce.stateSnapshot());
        }
    }

    private void verifySortedBatches(
        long seed,
        List<NormalizedMemoryEvent> sorted,
        int batchSize,
        TemporalState expectedFinal) {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        List<NormalizedMemoryEvent> prefix = new ArrayList<>();
        int batch = 0;
        for (int start = 0; start < sorted.size(); start += batchSize) {
            List<NormalizedMemoryEvent> next = sorted.subList(
                start, Math.min(start + batchSize, sorted.size()));
            prefix.addAll(next);
            verify(seed, "sorted-size-" + batchSize, batch++, prefix, integrator,
                () -> integrator.applyBatch(next));
        }
        verify(seed, "sorted-final-" + batchSize, batch, prefix, integrator,
            () -> Assertions.assertEquals(expectedFinal, integrator.stateSnapshot()));
    }

    private void verifyCrossKeyArrival(
        long seed,
        List<List<NormalizedMemoryEvent>> timelines,
        TemporalState expectedFinal) {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        SplittableRandom random = new SplittableRandom(seed ^ 0x5deece66dL);
        int[] positions = new int[KEY_COUNT];
        List<Integer> activeKeys = new ArrayList<>();
        for (int key = 0; key < KEY_COUNT; key++) {
            activeKeys.add(key);
        }
        List<NormalizedMemoryEvent> prefix = new ArrayList<>();
        Instant latest = Instant.MIN;
        boolean sawLateArrival = false;
        while (!activeKeys.isEmpty()) {
            int activeIndex = random.nextInt(activeKeys.size());
            int keyIndex = activeKeys.get(activeIndex);
            List<NormalizedMemoryEvent> timeline = timelines.get(keyIndex);
            NormalizedMemoryEvent next = timeline.get(positions[keyIndex]++);
            prefix.add(next);
            if (next.getRecordedAt().isBefore(latest)) {
                sawLateArrival = true;
            } else {
                latest = next.getRecordedAt();
            }
            verify(seed, "cross-key-arrival", prefix.size() - 1, prefix, integrator,
                () -> integrator.apply(next));
            if (positions[keyIndex] == timeline.size()) {
                activeKeys.remove(activeIndex);
            }
        }
        boolean lateCoverage = sawLateArrival;
        verify(seed, "cross-key-final", prefix.size(), prefix, integrator, () -> {
            Assertions.assertTrue(lateCoverage,
                "Cross-key arrival must exercise recordedAt lateness");
            Assertions.assertEquals(expectedFinal, integrator.stateSnapshot());
        });
    }

    private void verify(
        long seed,
        String route,
        int batch,
        List<NormalizedMemoryEvent> prefix,
        IncrementalTemporalIntegrator integrator,
        Runnable action) {
        TemporalState expected = null;
        try {
            action.run();
            expected = oracle.replayNormalized(prefix);
            Assertions.assertEquals(expected, integrator.stateSnapshot(),
                context(seed, route, batch, prefix));
        } catch (AssertionError | RuntimeException failure) {
            if (expected == null) {
                try {
                    expected = oracle.replayNormalized(prefix);
                } catch (RuntimeException oracleFailure) {
                    failure.addSuppressed(oracleFailure);
                }
            }
            saveFailure(seed, route, batch, prefix, expected, integrator.stateSnapshot(), failure);
            throw new AssertionError(context(seed, route, batch, prefix), failure);
        }
    }

    private static void saveFailure(
        long seed,
        String route,
        int batch,
        List<NormalizedMemoryEvent> prefix,
        TemporalState expected,
        TemporalState actual,
        Throwable failure) {
        StringBuilder report = new StringBuilder(context(seed, route, batch, prefix))
            .append('\n').append("failure=").append(failure).append('\n');
        for (NormalizedMemoryEvent event : prefix) {
            FactKey key = event.getFactKey();
            report.append("event=").append(event.getEventId())
                .append(" operation=").append(event.getOperation())
                .append(" factKey=").append(key.getSubjectId()).append('/')
                .append(key.getPredicate()).append('/').append(key.getScope())
                .append(" factId=").append(event.getFactId())
                .append(" value=").append(event.getFactValue()
                    .map(value -> value.getKind() + ":" + value.getValue()).orElse("<absent>"))
                .append(" valid=").append(event.getValidTime())
                .append(" recordedAt=").append(event.getRecordedAt())
                .append(" target=").append(event.getTargetVersionId().orElse("<absent>"))
                .append(" payloadHash=").append(event.getPayloadHash())
                .append(" evidence=").append(evidenceSummary(event.getEvidence())).append('\n');
        }
        appendState(report, "expected", expected);
        appendState(report, "actual", actual);
        try {
            Path module = Paths.get(System.getProperty("basedir", "."));
            if (Files.isDirectory(module.resolve("geaflow-ai/src/main/java"))) {
                module = module.resolve("geaflow-ai");
            }
            Path directory = module.resolve("target/temporal-failures");
            Files.createDirectories(directory);
            Files.write(directory.resolve(seed + "-" + route + "-" + batch + ".txt"),
                report.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException writeFailure) {
            failure.addSuppressed(writeFailure);
        }
    }

    private static void appendState(StringBuilder report, String label, TemporalState state) {
        report.append(label).append(':').append('\n');
        if (state == null) {
            report.append("<oracle did not produce a state>").append('\n');
            return;
        }
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            FactKey key = entry.getKey();
            for (MemoryFactVersion version : entry.getValue()) {
                report.append("version=").append(version.getId())
                    .append(" factKey=").append(key.getSubjectId()).append('/')
                    .append(key.getPredicate()).append('/').append(key.getScope())
                    .append(" factId=").append(version.getFact().getId())
                    .append(" status=").append(version.getStatus())
                    .append(" value=").append(
                        version.getFact().getLiteralValue().orElse("<absent>"))
                    .append(" valid=").append(version.getValidTime())
                    .append(" system=").append(version.getTransactionTime())
                    .append(" evidence=").append(evidenceSummary(version.getEvidence()))
                    .append('\n');
            }
        }
        for (VersionRelation relation : state.getRelations()) {
            report.append("relation=").append(relation.getType()).append(' ')
                .append(relation.getFromVersionId()).append(" -> ")
                .append(relation.getToVersionId()).append('\n');
        }
    }

    private static String evidenceSummary(List<Evidence> evidence) {
        return evidence.stream().map(item -> item.getId() + "/" + item.getSource().getId()
            + "/" + item.getSource().getName() + "/" + item.getContent())
            .collect(Collectors.joining(";"));
    }

    private List<List<NormalizedMemoryEvent>> timelines(long seed) {
        SplittableRandom random = new SplittableRandom(seed);
        List<List<NormalizedMemoryEvent>> timelines = new ArrayList<>();
        for (int index = 0; index < KEY_COUNT; index++) {
            FactKey key = new FactKey("person:" + (index / 2), "city",
                index % 2 == 0 ? "profile" : "account");
            String prefix = seed + "-key-" + index + "-";
            String valueA = "city-a-" + random.nextInt(3);
            String valueB = "city-b-" + random.nextInt(3);
            String valueC = "city-c-" + random.nextInt(3);
            long changeStart = 100 + random.nextInt(200);
            long changeEnd = 700 + random.nextInt(200);
            long withdrawStart = changeStart + 100;
            long withdrawEnd = changeEnd - 100;
            TimeInterval changedInterval = interval(changeStart, changeEnd);
            NormalizedMemoryEvent source = add(
                prefix + "1-source", key, valueA, interval(0, 1000), 10);
            String sourceId = VersionIdentity.create(source, source.getEvent().getFact().get(),
                MemoryFactVersionStatus.ACTIVE, source.getValidTime(), null);
            NormalizedMemoryEvent change = normalizer.normalize(MemoryEvent.correct(
                prefix + "2-change", fact(key, valueB), changedInterval, at(20),
                evidence(prefix + "2-change")).targeting(sourceId), key);
            String changedId = VersionIdentity.create(change, change.getEvent().getFact().get(),
                MemoryFactVersionStatus.ACTIVE, changedInterval, sourceId);
            NormalizedMemoryEvent withdraw = normalizer.normalize(MemoryEvent.retract(
                prefix + "3-withdraw", factId(key), interval(withdrawStart, withdrawEnd), at(30),
                evidence(prefix + "3-withdraw")).targeting(changedId), key);
            NormalizedMemoryEvent conflict = add(prefix + "4-conflict", key, valueC,
                interval(changeStart + 20, changeEnd - 20), 40);
            NormalizedMemoryEvent duplicateB = add(prefix + "5-duplicate-b", key, valueB,
                interval(changeStart + 10, withdrawStart - 10), 50);
            NormalizedMemoryEvent duplicateC = add(prefix + "6-duplicate-c", key, valueC,
                interval(changeStart - 50, changeEnd + 50), 60);
            TimeInterval duplicateAInterval = random.nextBoolean()
                ? interval(10, changeStart - 10) : interval(changeEnd + 10, 990);
            NormalizedMemoryEvent duplicateA = add(
                prefix + "7-duplicate-a", key, valueA, duplicateAInterval, 70);
            NormalizedMemoryEvent separate = add(prefix + "8-separate", key, "city-d",
                interval(1100 + random.nextInt(100), 1300 + random.nextInt(100)), 80);
            timelines.add(Arrays.asList(source, change, withdraw, conflict,
                duplicateB, duplicateC, duplicateA, separate));
        }
        return timelines;
    }

    private static void assertTemplateOutcome(TemporalState state, String context) {
        Assertions.assertEquals(KEY_COUNT, state.getVersionsByFactKey().size(), context);
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            List<MemoryFactVersion> versions = entry.getValue();
            Assertions.assertEquals(12, versions.size(), context);
            Assertions.assertEquals(1, versions.stream()
                .filter(version -> version.getStatus() == MemoryFactVersionStatus.RETRACTED)
                .count(), context);
            Assertions.assertEquals(6, versions.stream()
                .filter(version -> version.getStatus() == MemoryFactVersionStatus.ACTIVE)
                .filter(version -> !version.getTransactionTime().getEnd().isPresent())
                .count(), context);
            Assertions.assertEquals(5, versions.stream()
                .filter(version -> version.getTransactionTime().getEnd().isPresent())
                .count(), context);
        }
    }

    private NormalizedMemoryEvent add(
        String id, FactKey key, String value, TimeInterval valid, long recordedAt) {
        return normalizer.normalize(MemoryEvent.add(id, fact(key, value), valid, at(recordedAt),
            evidence(id)), key);
    }

    private static MemoryFact fact(FactKey key, String value) {
        return MemoryFact.attribute(factId(key), new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(), value);
    }

    private static String factId(FactKey key) {
        return "fact-" + key.getSubjectId() + "-" + key.getPredicate();
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("source", "generated-fixture"), "Evidence for " + id));
    }

    private static String context(
        long seed, String path, int batch, List<NormalizedMemoryEvent> prefix) {
        return "seed=" + seed + ", path=" + path + ", batch=" + batch + ", input="
            + prefix.stream().map(event -> event.getEventId() + "/" + event.getOperation()
                + "@" + event.getRecordedAt() + "[" + event.getFactKey().getScope() + "]")
                .collect(Collectors.joining(","));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
