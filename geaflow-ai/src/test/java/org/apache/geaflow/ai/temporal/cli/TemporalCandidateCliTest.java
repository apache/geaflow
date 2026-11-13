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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TemporalCandidateCliTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final TemporalJsonCodec codec = new TemporalJsonCodec();
    @TempDir
    Path directory;

    @Test
    public void testApiCliParityAndOldRunRemainsByteIdentical() throws Exception {
        TemporalMemoryFacade facade = populated();
        facade.query(QueryMode.CURRENT, KEY, at(50), null);
        Path run = directory.resolve("old run.json");
        TemporalRunStore store = new TemporalRunStore();
        store.save(run, facade);
        byte[] before = Files.readAllBytes(run);
        for (QueryMode mode : QueryMode.values()) {
            Instant systemAt = mode == QueryMode.AS_OF ? at(15) : null;
            String[] args = mode == QueryMode.AS_OF
                ? arguments(run, mode.name(), "--system-at", systemAt.toString())
                : arguments(run, mode.name());
            Invocation actual = invoke(args);
            Assertions.assertEquals(0, actual.code, actual.err);
            Assertions.assertEquals("", actual.err);
            Assertions.assertEquals(facade.candidates(mode, KEY, "renewable", at(50),
                systemAt, 20).toJson(), actual.json());
            Assertions.assertArrayEquals(before, Files.readAllBytes(run));
        }
        Invocation limited = invoke(arguments(run, "CURRENT", "--top-k", "1"));
        Assertions.assertEquals(0, limited.code, limited.err);
        Assertions.assertEquals(facade.candidates(QueryMode.CURRENT, KEY, "renewable", at(50),
            null, 1).toJson(), limited.json());
        TemporalMemoryFacade restored = store.load(run);
        Assertions.assertEquals(facade.getSnapshot(), restored.getSnapshot());
        Assertions.assertEquals(facade.getOperations(), restored.getOperations());
        Assertions.assertEquals(1, restored.getQueryTraces().size());
        Assertions.assertArrayEquals(before, Files.readAllBytes(run));
    }

    @Test
    public void testNoHitsAndScopeIsolation() throws Exception {
        Path run = saveRun();
        Invocation secret = invoke(replace(arguments(run, "CURRENT"), "--text", "secret"));
        Assertions.assertEquals(0, secret.code, secret.err);
        Assertions.assertEquals(1, secret.json().getAsJsonArray("facts").size());
        Assertions.assertEquals(0, secret.json().getAsJsonArray("evidence").size());
        Invocation missing = invoke(replace(arguments(run, "CURRENT"), "--scope", "missing"));
        Assertions.assertEquals(0, missing.code, missing.err);
        Assertions.assertEquals(0, missing.json().getAsJsonArray("facts").size());
        Assertions.assertEquals(0, missing.json().getAsJsonArray("evidence").size());
        Invocation past = invoke(arguments(run, "AS_OF", "--system-at", at(9).toString()));
        Assertions.assertEquals(0, past.code, past.err);
        Assertions.assertEquals(0, past.json().getAsJsonArray("facts").size());
        Assertions.assertEquals(0, past.json().getAsJsonArray("evidence").size());
    }

    @Test
    public void testStrictArgumentsRejectInputWithoutRewritingRun() throws Exception {
        Path run = saveRun();
        byte[] before = Files.readAllBytes(run);
        List<String[]> invalid = new ArrayList<>(Arrays.asList(
            arguments(run, "unknown"),
            arguments(run, "AS_OF"),
            arguments(run, "CURRENT", "--system-at", at(15).toString()),
            arguments(run, "CURRENT", "--top-k", "0"),
            arguments(run, "CURRENT", "--top-k", "-1"),
            arguments(run, "CURRENT", "--top-k", "1.5"),
            arguments(run, "CURRENT", "--top-k", "2147483648"),
            arguments(run, "CURRENT", "--unknown", "value"),
            arguments(run, "CURRENT", "--output", directory.resolve("result.json").toString()),
            arguments(run, "CURRENT", "--text", "duplicate"),
            replace(arguments(run, "CURRENT"), "--valid-at", "2024-01-01"),
            replace(arguments(run, "CURRENT"), "--text", "")));
        List<String> missing = new ArrayList<>(Arrays.asList(arguments(run, "CURRENT")));
        int textOption = missing.indexOf("--text");
        missing.remove(textOption);
        missing.remove(textOption);
        invalid.add(missing.toArray(new String[0]));
        for (String[] args : invalid) {
            Invocation failure = invoke(args);
            Assertions.assertEquals(2, failure.code, Arrays.toString(args) + failure.err);
            Assertions.assertEquals("", failure.out);
            Assertions.assertEquals("REJECT_INPUT", codec.parseObject(failure.err)
                .get("code").getAsString());
            Assertions.assertArrayEquals(before, Files.readAllBytes(run));
        }
    }

    @Test
    public void testMissingRunReportsExecutionFailure() throws Exception {
        Invocation failure = invoke(arguments(directory.resolve("missing.json"), "CURRENT"));
        Assertions.assertEquals(3, failure.code);
        Assertions.assertEquals("", failure.out);
        Assertions.assertEquals("REJECT_EXECUTION", codec.parseObject(failure.err)
            .get("code").getAsString());
    }

    @Test
    public void testLexicalProfileCliParityAndSavedRunStaysUnchanged() throws Exception {
        Path run = saveRun();
        byte[] before = Files.readAllBytes(run);
        TemporalMemoryFacade facade = new TemporalRunStore().load(run);
        final RetrievalProfile profile = RetrievalProfile.LEXICAL_FUSION;
        Invocation actual = invoke(arguments(run, "CURRENT", "--profile", profile.name()));
        Assertions.assertEquals(0, actual.code, actual.err);
        Assertions.assertEquals(facade.candidates(QueryMode.CURRENT, KEY, "renewable",
            at(50), null, 20, profile).toJson(), actual.json());
        Assertions.assertArrayEquals(before, Files.readAllBytes(run));
    }

    private Path saveRun() throws IOException {
        Path run = directory.resolve("run.json");
        new TemporalRunStore().save(run, populated());
        return run;
    }

    @Test
    public void testFourCandidateStrategyProfilesMatchApiWithoutWritingRun() throws Exception {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        String text = "renewable wind powers cities";
        facade.replay(Arrays.asList(add("visible", KEY, "Beijing", text, 10),
            add("hidden", new FactKey("alice", "city", "private"), "Rome", text, 20)));
        Path run = directory.resolve("candidate strategies.json");
        new TemporalRunStore().save(run, facade);
        byte[] before = Files.readAllBytes(run);
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            String[] args = replace(arguments(run, "CURRENT", "--profile", profile.name()), "--text", text);
            Invocation actual = invoke(args);
            Assertions.assertEquals(0, actual.code, actual.err);
            Assertions.assertEquals(facade.candidates(QueryMode.CURRENT, KEY, text,
                at(50), null, 20, profile).toJson(), actual.json());
            Assertions.assertArrayEquals(before, Files.readAllBytes(run));
        }
    }

    private static TemporalMemoryFacade populated() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        facade.replay(Arrays.asList(add("public", KEY, "Beijing", "renewable wind", 10),
            add("private", new FactKey("alice", "city", "private"), "Rome", "secret renewable", 20)));
        return facade;
    }

    private static NormalizedMemoryEvent add(String id, FactKey key, String city, String text,
                                              long recordedAt) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity(key.getSubjectId(), "person"), key.getPredicate(), city);
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact,
            new TimeInterval(BASE, at(100)), at(recordedAt), Collections.singletonList(
                new Evidence("e-" + id, new Source("registry", "Registry"), text))), key);
    }

    private static String[] arguments(Path run, String mode, String... extra) {
        List<String> arguments = new ArrayList<>(Arrays.asList("candidates", "--run", run.toString(),
            "--mode", mode, "--subject", "alice", "--predicate", "city", "--scope", "profile",
            "--valid-at", at(50).toString(), "--text", "renewable"));
        arguments.addAll(Arrays.asList(extra));
        return arguments.toArray(new String[0]);
    }

    private static String[] replace(String[] arguments, String option, String value) {
        String[] changed = arguments.clone();
        changed[Arrays.asList(changed).indexOf(option) + 1] = value;
        return changed;
    }

    private Invocation invoke(String... arguments) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = new TemporalMemoryCli().run(arguments,
            new PrintStream(out, true, "UTF-8"), new PrintStream(err, true, "UTF-8"));
        return new Invocation(code, out.toString("UTF-8"), err.toString("UTF-8"));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
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
