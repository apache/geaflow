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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.MapFunction;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TemporalEventPipelineTest {

    private static final int WINDOW_SIZE = 2;

    @TempDir
    Path tempDirectory;

    @Test
    public void testKeyedIncrementalAggregationAcrossWindows()
        throws Exception {
        List<MemoryEvent> events = pipelineEvents();
        Path output = tempDirectory.resolve("temporal-results.txt");
        Environment environment = null;

        try {
            environment = EnvironmentFactory.onLocalEnvironment();
            Pipeline pipeline =
                PipelineFactory.buildPipeline(environment);
            pipeline.submit(new TemporalPipelineTask(
                events,
                output.toString()));

            IPipelineResult<?> result = pipeline.execute();
            result.get();

            Assertions.assertTrue(result.isSuccess());
            List<String> actual = Files.readAllLines(
                output,
                StandardCharsets.UTF_8);
            Collections.sort(actual);
            Assertions.assertEquals(5, actual.size());
            Assertions.assertEquals(
                expectedWindowResults(events),
                actual);
        } finally {
            if (environment != null) {
                environment.shutdown();
            }
            ClusterMetaStore.close();
            ScheduledWorkerManagerFactory.clear();
        }
    }

    private static List<MemoryEvent> pipelineEvents() {
        MemoryEvent bobAdd = addEvent(
            "event-bob-add",
            "fact-bob-city",
            "person:bob",
            "Paris",
            "2024-01-01T00:00:00Z",
            "2024-04-01T00:00:00Z");
        List<MemoryEvent> events = new ArrayList<>();
        events.add(addEvent(
            "event-alice-add",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z"));
        events.add(bobAdd);
        events.add(correctEvent(
            "event-alice-correct",
            "fact-alice-city",
            "person:alice",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z"));
        events.add(bobAdd);
        events.add(retractEvent(
            "event-alice-retract",
            "fact-alice-city",
            "2024-08-01T00:00:00Z",
            "2024-10-01T00:00:00Z",
            "2024-11-01T00:00:00Z"));
        events.add(correctEvent(
            "event-alice-late",
            "fact-alice-city",
            "person:alice",
            "Tianjin",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "2024-05-01T00:00:00Z"));
        return events;
    }

    private static List<String> expectedWindowResults(
        List<MemoryEvent> events) {
        FullReplayOracle oracle = new FullReplayOracle();
        Map<String, List<MemoryEvent>> receivedByFact =
            new HashMap<>();
        List<String> expected = new ArrayList<>();

        for (int start = 0;
            start < events.size();
            start += WINDOW_SIZE) {
            Set<String> touchedFactIds = new LinkedHashSet<>();
            int end = Math.min(start + WINDOW_SIZE, events.size());
            for (int index = start; index < end; index++) {
                MemoryEvent event = events.get(index);
                receivedByFact.computeIfAbsent(
                    event.getFactId(),
                    ignored -> new ArrayList<>()).add(event);
                touchedFactIds.add(event.getFactId());
            }
            for (String factId : touchedFactIds) {
                expected.add(formatSnapshot(
                    oracle.replay(receivedByFact.get(factId))));
            }
        }

        Collections.sort(expected);
        return expected;
    }

    private static String formatSnapshot(
        List<MemoryFactVersion> versions) {
        StringBuilder builder = new StringBuilder();
        for (MemoryFactVersion version : versions) {
            if (builder.length() > 0) {
                builder.append(';');
            }
            builder.append(version.getId())
                .append('|')
                .append(version.getFact().getId())
                .append('|')
                .append(version.getFact().getLiteralValue().get())
                .append('|')
                .append(version.getValidTime())
                .append('|')
                .append(version.getTransactionTime());
        }
        return builder.toString();
    }

    private static MemoryEvent addEvent(
        String eventId,
        String factId,
        String subjectId,
        String value,
        String validStart,
        String transactionTime) {
        return MemoryEvent.add(
            eventId,
            fact(factId, subjectId, value),
            TimeInterval.unboundedFrom(time(validStart)),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent correctEvent(
        String eventId,
        String factId,
        String subjectId,
        String value,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.correct(
            eventId,
            fact(factId, subjectId, value),
            new TimeInterval(time(validStart), time(validEnd)),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent retractEvent(
        String eventId,
        String factId,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.retract(
            eventId,
            factId,
            new TimeInterval(time(validStart), time(validEnd)),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryFact fact(
        String factId,
        String subjectId,
        String value) {
        return MemoryFact.attribute(
            factId,
            new MemoryEntity(subjectId, "person"),
            "city",
            value);
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "customer-database"),
            "Evidence for " + eventId));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }

    private static final class TemporalPipelineTask
        implements PipelineTask {

        private final List<MemoryEvent> events;
        private final String outputPath;

        private TemporalPipelineTask(
            List<MemoryEvent> events,
            String outputPath) {
            this.events = new ArrayList<>(events);
            this.outputPath = outputPath;
        }

        @Override
        public void execute(
            IPipelineTaskContext pipelineTaskContext) {
            PWindowSource<MemoryEvent> source =
                pipelineTaskContext.buildSource(
                    new CollectionSource<>(events),
                    SizeTumblingWindow.of(WINDOW_SIZE));
            source.withParallelism(1)
                .keyBy(new FactIdSelector())
                .aggregate(new TemporalEventAggregateFunction())
                .withParallelism(2)
                .map(new SnapshotFormatter())
                .sink(new LineFileSink(outputPath))
                .withParallelism(1);
        }
    }

    private static final class FactIdSelector implements
        KeySelector<MemoryEvent, String> {

        @Override
        public String getKey(MemoryEvent event) {
            return event.getFactId();
        }
    }

    private static final class SnapshotFormatter implements
        MapFunction<List<MemoryFactVersion>, String> {

        @Override
        public String map(List<MemoryFactVersion> versions) {
            return formatSnapshot(versions);
        }
    }

    private static final class LineFileSink implements
        SinkFunction<String> {

        private final String outputPath;

        private LineFileSink(String outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public void write(String value) throws Exception {
            Files.write(
                Paths.get(outputPath),
                Collections.singletonList(value),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        }
    }
}
