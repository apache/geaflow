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

package org.apache.geaflow.ai.temporal.cli;

import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TemporalMemoryCliTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final TemporalJsonCodec codec = new TemporalJsonCodec();
    private final EventNormalizer normalizer = new EventNormalizer();
    @TempDir
    Path directory;

    @Test
    public void testFourCommandsInFreshProcessesAndThreeQueryModes() throws Exception {
        NormalizedMemoryEvent first = assertion("first", "北京", 10);
        NormalizedMemoryEvent correction = normalizer.normalize(MemoryEvent.correct("change",
            fact("罗马"), interval(30, 70), at(30), evidence("change"))
            .targeting(versionId(first)), KEY);
        Path input = input("events with spaces.jsonl", correction, first, first);
        Path run = directory.resolve("run.json");
        Invocation replay = child("replay", "--input", input.toString(), "--output", run.toString());
        Assertions.assertEquals(0, replay.code, replay.err);
        Assertions.assertEquals("FULL_REPLAY_MEMORY_GRAPH", replay.json().get("method").getAsString());
        Assertions.assertEquals(1, replay.json().getAsJsonObject("statistics")
            .get("duplicateCount").getAsInt());

        Path queryFile = directory.resolve("query.json");
        Invocation current = child(queryArgs(run, "CURRENT", "--output", queryFile.toString()));
        Assertions.assertEquals(0, current.code, current.err);
        Assertions.assertEquals("罗马", current.json().getAsJsonArray("versions").get(0)
            .getAsJsonObject().getAsJsonObject("value").get("value").getAsString());
        Assertions.assertEquals(current.json(), codec.parseObject(read(queryFile)));
        Invocation historical = child(queryArgs(run, "HISTORICAL"));
        Assertions.assertEquals(0, historical.code, historical.err);
        Assertions.assertEquals(2, historical.json().getAsJsonArray("versions").size());
        Invocation asOf = child(queryArgs(run, "AS_OF", "--system-at", at(20).toString()));
        Assertions.assertEquals(0, asOf.code, asOf.err);
        Assertions.assertEquals("北京", asOf.json().getAsJsonArray("versions").get(0)
            .getAsJsonObject().getAsJsonObject("value").get("value").getAsString());

        Invocation trace = child("trace", "--run", run.toString(), "--id", "query-3");
        Assertions.assertEquals(0, trace.code, trace.err);
        Assertions.assertEquals(asOf.json().get("trace"), trace.json()
            .getAsJsonArray("queryTraces").get(0));
        Invocation event = child("trace", "--run", run.toString(), "--id", "first");
        Assertions.assertEquals(0, event.code, event.err);
        Assertions.assertEquals(2, event.json().getAsJsonArray("decisionTraces").size());
        Invocation attempt = child("trace", "--run", run.toString(), "--id", "attempt-1");
        Assertions.assertEquals(0, attempt.code, attempt.err);
        Assertions.assertEquals(1, attempt.json().getAsJsonArray("decisionTraces").size());
        Invocation unknown = child("trace", "--run", run.toString(), "--id", "unknown");
        Assertions.assertEquals(0, unknown.code, unknown.err);
        Assertions.assertEquals(0, unknown.json().getAsJsonArray("decisionTraces").size());
        Assertions.assertEquals(0, unknown.json().getAsJsonArray("queryTraces").size());
        Invocation validate = child("validate", "--run", run.toString());
        Assertions.assertEquals(0, validate.code, validate.err);
        Assertions.assertTrue(validate.json().get("valid").getAsBoolean());
        Assertions.assertEquals(3, codec.parseObject(read(run)).getAsJsonArray("queryTraces").size());
    }

    @Test
    public void testMainExitCodesForInvalidArgumentsAndIoFailure() throws Exception {
        Invocation missing = child("replay", "--input", directory.resolve("events.jsonl").toString());
        Assertions.assertEquals(2, missing.code);
        Assertions.assertEquals("REJECT_INPUT", codec.parseObject(missing.err).get("code").getAsString());
        Invocation nonexistent = child("validate", "--run", directory.resolve("missing.json").toString());
        Assertions.assertEquals(3, nonexistent.code);
        Assertions.assertEquals("REJECT_EXECUTION",
            codec.parseObject(nonexistent.err).get("code").getAsString());
        Assertions.assertEquals("", nonexistent.out);
    }

    @Test
    public void testStrictOptionsAndPathCollisionsPreserveFiles() throws Exception {
        Path input = input("input.jsonl", assertion("first", "Beijing", 10));
        String inputBefore = read(input);
        Assertions.assertEquals(2, invoke("replay", "--input", input.toString(), "--output",
            directory.resolve("subfolder/../input.jsonl").toString()).code);
        Assertions.assertEquals(inputBefore, read(input));
        Path run = directory.resolve("run.json");
        Assertions.assertEquals(0, invoke("replay", "--input", input.toString(), "--output",
            run.toString()).code);
        String before = read(run);
        List<String[]> commands = Arrays.asList(
            queryArgs(run, "CURRENT", "--output", run.toString()),
            queryArgs(run, "CURRENT", "--system-at", at(20).toString()),
            queryArgs(run, "AS_OF"), queryArgs(run, "current"),
            new String[] {"validate", "--run", run.toString(), "--unknown", "x"},
            new String[] {"validate", "--run", run.toString(), "--run", run.toString()},
            new String[] {"trace", "--run", run.toString(), "--id"},
            new String[] {"validate", "run", run.toString()});
        for (String[] command : commands) {
            Invocation failure = invoke(command);
            Assertions.assertEquals(2, failure.code, Arrays.toString(command));
            Assertions.assertEquals("", failure.out);
            Assertions.assertEquals(before, read(run));
        }
        Assertions.assertEquals(2, invoke(new String[0]).code);
        Path outputDirectory = Files.createDirectory(directory.resolve("output-directory"));
        Assertions.assertEquals(3, invoke("replay", "--input", input.toString(), "--output",
            outputDirectory.toString()).code);
        Assertions.assertTrue(Files.isDirectory(outputDirectory));
    }

    @Test
    public void testWholeJsonlReplayAndHashValidationAreAtomic() throws Exception {
        NormalizedMemoryEvent first = assertion("first", "Beijing", 10);
        Path good = input("good.jsonl", first);
        Path run = directory.resolve("run.json");
        Assertions.assertEquals(0, invoke("replay", "--input", good.toString(), "--output",
            run.toString()).code);
        String before = read(run);
        Path bad = directory.resolve("bad.jsonl");
        write(bad, codec.toJson(codec.eventToJson(first)) + "\n{broken}\n");
        Invocation syntax = invoke("replay", "--input", bad.toString(), "--output", run.toString());
        Assertions.assertEquals(2, syntax.code);
        Assertions.assertEquals(2, codec.parseObject(syntax.err).get("line").getAsInt());
        Assertions.assertEquals(before, read(run));
        NormalizedMemoryEvent missingTarget = normalizer.normalize(MemoryEvent.retract("invalid",
            "fact-city", interval(30, 40), at(30), evidence("invalid")).targeting("missing"), KEY);
        input("bad.jsonl", first, missingTarget);
        Invocation semantic = invoke("replay", "--input", bad.toString(), "--output", run.toString());
        Assertions.assertEquals(2, semantic.code);
        Assertions.assertEquals("REJECT_TARGET_NOT_FOUND",
            codec.parseObject(semantic.err).get("code").getAsString());
        Assertions.assertEquals(2, codec.parseObject(semantic.err)
            .getAsJsonArray("decisionTraces").size());
        Assertions.assertEquals("BATCH_ROLLED_BACK", codec.parseObject(semantic.err)
            .getAsJsonArray("decisionTraces").get(0).getAsJsonObject().get("code").getAsString());
        Assertions.assertEquals(before, read(run));
        JsonObject tampered = codec.eventToJson(first);
        tampered.getAsJsonObject("value").addProperty("value", "Shanghai");
        write(bad, codec.toJson(tampered) + "\n");
        Invocation hash = invoke("replay", "--input", bad.toString(), "--output", run.toString());
        Assertions.assertEquals(2, hash.code);
        Assertions.assertTrue(codec.parseObject(hash.err).get("message").getAsString()
            .contains("payloadHash"));
        Assertions.assertEquals(before, read(run));
        Path newRun = directory.resolve("not-created.json");
        Assertions.assertEquals(2, invoke("replay", "--input", bad.toString(), "--output",
            newRun.toString()).code);
        Assertions.assertFalse(Files.exists(newRun));
    }

    @Test
    public void testRunStoreRestoresRejectedBatchesAndQueries() throws Exception {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        NormalizedMemoryEvent first = assertion("first", "Beijing", 10);
        facade.replay(Collections.singletonList(first));
        facade.applyBatch(Collections.singletonList(first));
        NormalizedMemoryEvent invalid = normalizer.normalize(MemoryEvent.retract("invalid",
            "fact-city", interval(30, 40), at(30), evidence("invalid")).targeting("missing"), KEY);
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.applyBatch(Arrays.asList(assertion("staged", "Rome", 20), invalid)));
        facade.query(QueryMode.AS_OF, KEY, at(40), at(20));
        Path run = directory.resolve("nested/run.json");
        TemporalRunStore store = new TemporalRunStore();
        store.save(run, facade);
        TemporalMemoryFacade restored = store.load(run);
        Assertions.assertEquals(facade.getSnapshot(), restored.getSnapshot());
        Assertions.assertEquals(facade.getOperations(), restored.getOperations());
        Assertions.assertEquals(codec.toJson(codec.parseObject(read(run))),
            codec.toJson(codec.parseObject(read(saveCopy(store, restored)))));
        Assertions.assertEquals(2, restored.getTrace("first").size());
        Assertions.assertEquals("BATCH_ROLLED_BACK", restored.getTrace("invalid").get(0).getCode());
        Assertions.assertEquals("query-1", restored.getQueryTrace("query-1").getId());
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            Assertions.assertTrue(paths.noneMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    @Test
    public void testCorruptOrLegacyArtifactsAreRejectedWithoutRewriting() throws Exception {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        facade.replay(Collections.singletonList(assertion("first", "Beijing", 10)));
        facade.query(QueryMode.CURRENT, KEY, at(40), null);
        Path run = directory.resolve("run.json");
        new TemporalRunStore().save(run, facade);
        String clean = read(run);
        List<Consumer<JsonObject>> corruptions = Arrays.asList(
            value -> value.addProperty("unknown", true),
            value -> value.addProperty("artifactVersion", 2),
            value -> value.addProperty("schemaVersion", 1),
            value -> value.getAsJsonObject("snapshot").addProperty("unknown", true),
            value -> value.getAsJsonArray("decisionTraces").get(0).getAsJsonObject()
                .addProperty("code", "DUPLICATE_NOOP"),
            value -> value.getAsJsonArray("queryTraces").get(0).getAsJsonObject()
                .addProperty("unknown", true),
            value -> value.getAsJsonArray("operations").get(0).getAsJsonObject()
                .addProperty("expectedError", "REJECT_TARGET_NOT_FOUND"),
            value -> value.getAsJsonArray("operations").get(0).getAsJsonObject()
                .addProperty("unknown", true),
            value -> value.getAsJsonArray("operations").get(0).getAsJsonObject()
                .getAsJsonArray("events").get(0).getAsJsonObject().remove("payloadHash"),
            value -> value.getAsJsonArray("operations").get(0).getAsJsonObject()
                .getAsJsonArray("events").get(0).getAsJsonObject().getAsJsonObject("value")
                .addProperty("value", "Rome"));
        for (Consumer<JsonObject> corruption : corruptions) {
            JsonObject changed = codec.parseObject(clean);
            corruption.accept(changed);
            write(run, codec.toJson(changed));
            String before = read(run);
            Invocation failure = invoke(queryArgs(run, "CURRENT"));
            Assertions.assertEquals(2, failure.code, failure.err);
            Assertions.assertEquals("", failure.out);
            Assertions.assertEquals(before, read(run));
        }
    }

    private Path saveCopy(TemporalRunStore store, TemporalMemoryFacade facade) throws IOException {
        Path copy = directory.resolve("copy.json");
        store.save(copy, facade);
        return copy;
    }

    private Invocation invoke(String... arguments) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = new TemporalMemoryCli().run(arguments,
            new PrintStream(out, true, "UTF-8"), new PrintStream(err, true, "UTF-8"));
        return new Invocation(code, out.toString("UTF-8"), err.toString("UTF-8"));
    }

    private Invocation child(String... arguments) throws Exception {
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        List<String> command = new ArrayList<>(Arrays.asList(Paths.get(System.getProperty("java.home"),
            "bin", executable).toString(), "-cp", classpathJar().toString(),
            TemporalMemoryCli.class.getName()));
        command.addAll(Arrays.asList(arguments));
        Path out = Files.createTempFile(directory, "child-", ".out");
        Path err = Files.createTempFile(directory, "child-", ".err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile())
            .redirectError(err.toFile()).start();
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        Assertions.assertTrue(finished, "CLI process exceeded 30 seconds");
        return new Invocation(process.exitValue(), read(out), read(err));
    }

    private Path classpathJar() throws IOException {
        StringBuilder classpath = new StringBuilder();
        for (String entry : System.getProperty("java.class.path").split(
            Pattern.quote(File.pathSeparator), -1)) {
            classpath.append(Paths.get(entry.isEmpty() ? "." : entry).toAbsolutePath()
                .toUri().toASCIIString()).append(' ');
        }
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classpath.toString().trim());
        Path jar = Files.createTempFile(directory, "classpath-", ".jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            output.finish();
        }
        return jar;
    }

    private String[] queryArgs(Path run, String mode, String... extras) {
        List<String> arguments = new ArrayList<>(Arrays.asList("query", "--run", run.toString(),
            "--mode", mode, "--subject", "alice", "--predicate", "city", "--scope", "profile",
            "--valid-at", at(40).toString()));
        arguments.addAll(Arrays.asList(extras));
        return arguments.toArray(new String[0]);
    }

    private Path input(String name, NormalizedMemoryEvent... events) throws IOException {
        StringBuilder jsonl = new StringBuilder();
        for (NormalizedMemoryEvent event : events) {
            jsonl.append(codec.toJson(codec.eventToJson(event))).append('\n');
        }
        Path path = directory.resolve(name);
        write(path, jsonl.toString());
        return path;
    }

    private NormalizedMemoryEvent assertion(String id, String value, long recorded) {
        return normalizer.normalize(MemoryEvent.add(id, fact(value), interval(0, 100), at(recorded),
            evidence(id)), KEY);
    }

    private static String versionId(NormalizedMemoryEvent event) {
        return VersionIdentity.create(event, event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, event.getValidTime(), null);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity("alice", "person"), "city", value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("registry", "Registry"), "Evidence for " + id));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }

    private static void write(Path path, String value) throws IOException {
        Files.write(path, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private final class Invocation {

        private final int code;
        private final String out;
        private final String err;

        private Invocation(int code, String out, String err) {
            this.code = code;
            this.out = out;
            this.err = err;
        }

        private JsonObject json() {
            return codec.parseObject(out);
        }
    }
}
