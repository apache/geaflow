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

package org.apache.geaflow.ai.temporal.udga;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.state.KeyValueState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Single-file acceptance for business windows, committed recovery and the delivered JAR. */
public class TemporalUdgaIntegrationTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path directory;

    @Test
    public void businessWindowsAndTransactionsPreserveHistoryAndRollback() throws Exception {
        List<NormalizedMemoryEvent> events = ChildProcess.events();
        FactKey key = events.get(0).getFactKey();
        List<String> rows = TemporalLocalRunner.run(directory.resolve("windows"),
            Collections.singletonMap(key, FactKeyState.empty(key)), events, 1);
        int previousRows = 0;
        int results = 0;
        for (String row : rows) {
            String[] fields = row.split(",", -1);
            Assertions.assertEquals(3, fields.length);
            if ("PREVIOUS_ROW".equals(fields[2])) {
                previousRows++;
            } else if ("RESULT".equals(fields[2])) {
                FactKeyState state = FactKeyState.decode(fields[1]);
                results++;
                assertSnapshot(new FullReplayOracle().replayToSnapshot(events.subList(0,
                    state.getSnapshot().getEvents().size())), state.getSnapshot());
            }
        }
        Assertions.assertEquals(5, results);
        Assertions.assertEquals(4, previousRows);

        TemporalUdgaSession session = new TemporalUdgaSession(directory.resolve("session"));
        session.applyBatch(events.subList(0, 2));
        Path checkpoint = directory.resolve("committed.json");
        session.save(checkpoint);
        session = TemporalUdgaSession.load(directory.resolve("resumed"), checkpoint);
        String before = session.checkpoint();
        JsonObject invalid = editable(events.get(2));
        invalid.addProperty("target", "missing-version");
        NormalizedMemoryEvent rejected = new TemporalJsonCodec().decodeEvent(invalid);
        TemporalUdgaSession resumed = session;
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> resumed.applyBatch(Arrays.asList(events.get(3), rejected)));
        Assertions.assertEquals(before, session.checkpoint());
        session.applyBatch(events.subList(2, 5));
        CanonicalSnapshot expected = new FullReplayOracle().replayToSnapshot(events);
        assertSnapshot(expected, session.getSnapshot());
        Assertions.assertEquals(ChildProcess.queries(expected),
            ChildProcess.queries(session.getSnapshot()));

        String committed = session.checkpoint();
        session.applyBatch(events);
        Assertions.assertEquals(committed, session.checkpoint());
        JsonObject account = editable(events.get(0));
        account.addProperty("scope", "account");
        NormalizedMemoryEvent reused = new TemporalJsonCodec().decodeEvent(account);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> resumed.applyBatch(Collections.singletonList(reused)));
        Assertions.assertEquals(committed, session.checkpoint());
        account.addProperty("id", "account-assert");
        JsonObject conflicting = new TemporalJsonCodec().parseObject(account.toString());
        conflicting.getAsJsonArray("evidence").get(0).getAsJsonObject()
            .addProperty("content", "Conflicting content under the existing evidence ID");
        NormalizedMemoryEvent conflictingEvidence =
            new TemporalJsonCodec().decodeEvent(conflicting);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> resumed.applyBatch(Collections.singletonList(conflictingEvidence)));
        Assertions.assertEquals(committed, session.checkpoint(),
            "Graph publication failure must discard the executed account Row");
        NormalizedMemoryEvent separate = new TemporalJsonCodec().decodeEvent(account);
        session.applyBatch(Collections.singletonList(separate));
        List<NormalizedMemoryEvent> accepted = new ArrayList<>(events);
        accepted.add(separate);
        expected = new FullReplayOracle().replayToSnapshot(accepted);
        session.save(checkpoint);
        TemporalUdgaSession restored = TemporalUdgaSession.load(directory, checkpoint);
        assertSnapshot(expected, restored.getSnapshot());
        Assertions.assertEquals(ChildProcess.queries(expected),
            ChildProcess.queries(restored.getSnapshot()));
        JsonObject corrupt = new TemporalJsonCodec().parseObject(session.checkpoint());
        corrupt.add("rows", new JsonArray());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> TemporalUdgaSession.restore(directory, corrupt.toString()));
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS,
        disabledReason = "GeaFlow LOCAL persistence requires Hadoop winutils.exe")
    public void archivedBusinessRowSurvivesStoreReopenAndDiscardsUncommittedUpdates()
        throws Exception {
        List<NormalizedMemoryEvent> events = ChildProcess.events();
        FactKey key = events.get(0).getFactKey();
        FactKeyState first = FactKeyState.empty(key).applyBatch(events.subList(0, 2));
        Map<String, String> config = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "TemporalBusinessRecovery");
        config.put(ExecutionConfigKeys.JOB_WORK_PATH.getKey(), directory.resolve("work").toString());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), directory.resolve("checkpoints").toString());
        KeyValueStateDescriptor<String, Row> descriptor =
            KeyValueStateDescriptor.build("business-row", StoreType.ROCKSDB.name());
        descriptor.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyValueState<String, Row> store = StateFactory.buildKeyValueState(
            descriptor, new Configuration(config));
        try {
            store.manage().operate().setCheckpointId(1L);
            store.put("alice", first.toRow());
            store.manage().operate().finish();
            store.manage().operate().archive();
            store.put("alice", first.applyBatch(events.subList(2, 5)).toRow());
        } finally {
            store.manage().operate().close();
            store.manage().operate().drop();
        }
        KeyValueState<String, Row> recovered = StateFactory.buildKeyValueState(
            descriptor, new Configuration(config));
        try {
            recovered.manage().operate().setCheckpointId(1L);
            recovered.manage().operate().recover();
            FactKeyState restored = FactKeyState.fromRow(key, recovered.get("alice"));
            Assertions.assertEquals(first.getDigest(), restored.getDigest());
            CanonicalSnapshot continued = restored.applyBatch(events.subList(2, 5)).getSnapshot();
            CanonicalSnapshot expected = new FullReplayOracle().replayToSnapshot(events);
            assertSnapshot(expected, continued);
            Assertions.assertEquals(ChildProcess.queries(expected), ChildProcess.queries(continued));
        } finally {
            recovered.manage().operate().close();
            recovered.manage().operate().drop();
        }
    }

    @Test
    @Tag("udga-packaged")
    public void packagedApplicationContinuesCommittedStateInIndependentJvms() throws Exception {
        Path application = Paths.get(System.getProperty("temporal.udga.jar")).toRealPath();
        Assertions.assertTrue(Files.size(application) > 0);
        Path helper = helperJar();
        Path checkpoint = directory.resolve("committed.json");
        JsonObject first = child("write", application, helper, checkpoint);
        JsonObject second = child("resume", application, helper, checkpoint);
        Assertions.assertNotEquals(first.get("processId"), second.get("processId"));
        Assertions.assertEquals(application,
            Paths.get(URI.create(first.get("codeSource").getAsString())).toRealPath());
        Assertions.assertEquals(application,
            Paths.get(URI.create(second.get("codeSource").getAsString())).toRealPath());
        List<NormalizedMemoryEvent> events = ChildProcess.events();
        checkReport(first, new FullReplayOracle().replayToSnapshot(events.subList(0, 2)));
        checkReport(second, new FullReplayOracle().replayToSnapshot(events));
        CanonicalSnapshot restored = TemporalUdgaSession.load(directory, checkpoint).getSnapshot();
        Assertions.assertEquals(second.get("snapshot"),
            new TemporalJsonCodec().snapshotToJson(restored));
        Assertions.assertEquals(second.get("queries"), ChildProcess.queries(restored));
    }

    private static JsonObject editable(NormalizedMemoryEvent event) {
        JsonObject value = new TemporalJsonCodec().eventToJson(event);
        value.remove("payloadHash");
        return value;
    }

    private static void assertSnapshot(CanonicalSnapshot expected, CanonicalSnapshot actual) {
        TemporalJsonCodec codec = new TemporalJsonCodec();
        Assertions.assertEquals(codec.snapshotToJson(expected), codec.snapshotToJson(actual));
    }

    private static void checkReport(JsonObject actual, CanonicalSnapshot expected) {
        Assertions.assertEquals(new TemporalJsonCodec().snapshotToJson(expected),
            actual.get("snapshot"), "Packaged snapshot must retain history and provenance");
        Assertions.assertEquals(816, actual.getAsJsonArray("queries").size());
        Assertions.assertEquals(ChildProcess.queries(expected), actual.get("queries"));
    }

    private JsonObject child(String mode, Path application, Path helper, Path checkpoint)
        throws Exception {
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        Path java = Paths.get(System.getProperty("java.home"), "bin", executable);
        Path output = directory.resolve(mode + ".json");
        Path log = directory.resolve(mode + ".log");
        Process process = new ProcessBuilder(java.toString(), "-Duser.timezone=UTC", "-cp",
            helper + File.pathSeparator + application, ChildProcess.class.getName(),
            mode, checkpoint.toString(), directory.resolve(mode + "-work").toString(),
            output.toString()).directory(directory.toFile()).redirectErrorStream(true)
            .redirectOutput(log.toFile()).start();
        try {
            Assertions.assertTrue(process.waitFor(120, TimeUnit.SECONDS),
                "Child JVM exceeded two minutes, log at " + log);
            Assertions.assertEquals(0, process.exitValue(), "Child JVM failed, log at " + log);
            return new TemporalJsonCodec().parseObject(new String(Files.readAllBytes(output),
                StandardCharsets.UTF_8));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private Path helperJar() throws Exception {
        Path jar = directory.resolve("child-helper.jar");
        String resource = ChildProcess.class.getName().replace('.', '/') + ".class";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource);
             JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            Assertions.assertNotNull(input);
            output.putNextEntry(new JarEntry(resource));
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                output.write(buffer, 0, count);
            }
            output.closeEntry();
        }
        return jar;
    }

    /** JUnit-free helper bytecode extracted from this file for the two child JVMs. */
    public static final class ChildProcess {

        private ChildProcess() {
        }

        static List<NormalizedMemoryEvent> events() throws Exception {
            List<NormalizedMemoryEvent> events = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TemporalUdgaSession.class.getResourceAsStream("/temporal/example-events.jsonl"),
                StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    events.add(new TemporalJsonCodec().decodeEvent(line));
                }
            }
            return events;
        }

        public static void main(String[] arguments) throws Exception {
            Path checkpoint = Paths.get(arguments[1]);
            Path work = Paths.get(arguments[2]);
            List<NormalizedMemoryEvent> events = events();
            TemporalUdgaSession session;
            if ("write".equals(arguments[0])) {
                session = new TemporalUdgaSession(work);
                session.applyBatch(events.subList(0, 2));
            } else if ("resume".equals(arguments[0])) {
                session = TemporalUdgaSession.load(work, checkpoint);
                session.applyBatch(events.subList(2, events.size()));
            } else {
                throw new IllegalArgumentException("Unknown child mode");
            }
            session.save(checkpoint);
            JsonObject result = new JsonObject();
            result.addProperty("processId", ManagementFactory.getRuntimeMXBean().getName());
            result.addProperty("codeSource", TemporalUdgaSession.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI().toString());
            result.add("snapshot", new TemporalJsonCodec().snapshotToJson(session.getSnapshot()));
            result.add("queries", queries(session.getSnapshot()));
            Files.write(Paths.get(arguments[3]), result.toString().getBytes(StandardCharsets.UTF_8));
        }

        static JsonArray queries(CanonicalSnapshot snapshot) {
            TemporalQueryService query = new TemporalQueryService();
            JsonArray results = new JsonArray();
            String[] validDates = {"2024-01-01", "2024-01-10", "2024-01-12", "2024-01-15",
                "2024-01-20", "2024-01-22", "2024-01-25", "2024-02-01"};
            String[] systemDays = {"01", "02", "03", "04", "05"};
            for (String scope : new String[] {"profile", "account"}) {
                FactKey key = new FactKey("alice", "city", scope);
                for (String date : validDates) {
                    Instant boundary = Instant.parse(date + "T00:00:00Z");
                    for (long offset : new long[] {-1, 0, 1}) {
                        Instant validAt = boundary.plusNanos(offset);
                        results.add(versions(snapshot,
                            query.current(snapshot.getState(), key, validAt)));
                        results.add(versions(snapshot,
                            query.historical(snapshot.getState(), key, validAt)));
                        for (String day : systemDays) {
                            Instant system = Instant.parse("2024-03-" + day + "T00:00:00Z");
                            for (long systemOffset : new long[] {-1, 0, 1}) {
                                results.add(versions(snapshot, query.asOf(snapshot.getState(), key,
                                    validAt, system.plusNanos(systemOffset))));
                            }
                        }
                    }
                }
            }
            return results;
        }

        private static JsonArray versions(CanonicalSnapshot snapshot,
                                          List<MemoryFactVersion> versions) {
            JsonArray result = new JsonArray();
            TemporalJsonCodec codec = new TemporalJsonCodec();
            for (MemoryFactVersion version : versions) {
                FactKey key = snapshot.getEvents().stream().filter(event -> event.getEventId()
                    .equals(snapshot.getGeneratingEventIds().get(version.getId())))
                    .findFirst().get().getFactKey();
                result.add(codec.versionToJson(key, version,
                    snapshot.getGeneratingEventIds().get(version.getId())));
            }
            return result;
        }
    }
}
