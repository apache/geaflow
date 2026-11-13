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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.QueryResult;
import org.apache.geaflow.ai.temporal.api.ReplayResult;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.api.ValidationResult;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.apache.geaflow.ai.temporal.trace.QueryTrace;

/** Independent finite-file CLI; each invocation restores its run from disk. */
public final class TemporalMemoryCli {

    private final TemporalJsonCodec codec = new TemporalJsonCodec();
    private final TemporalRunStore store = new TemporalRunStore();

    public static void main(String[] args) throws IOException {
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
        PrintStream err = new PrintStream(System.err, true, StandardCharsets.UTF_8.name());
        System.exit(new TemporalMemoryCli().run(args, out, err));
    }

    public int run(String[] args, PrintStream out, PrintStream err) {
        try {
            if (args == null || args.length == 0) {
                throw invalid("Expected command: replay, query, candidates, trace, or validate");
            }
            JsonObject result;
            switch (args[0]) {
                case "replay":
                    result = replay(options(args, "input", "output", "candidate-profile", "candidate-top-k"));
                    break;
                case "query":
                    result = query(options(args, "run", "mode", "subject", "predicate", "scope",
                        "valid-at", "system-at", "output"));
                    break;
                case "candidates":
                    result = candidates(options(args, "run", "mode", "subject", "predicate",
                        "scope", "valid-at", "system-at", "text", "top-k", "profile"));
                    break;
                case "trace":
                    result = trace(options(args, "run", "id"));
                    break;
                case "validate":
                    result = validate(options(args, "run"));
                    break;
                default:
                    throw invalid("Unknown command: " + args[0]);
            }
            out.println(codec.toJson(result));
            return "validate".equals(args[0]) && !result.get("valid").getAsBoolean() ? 3 : 0;
        } catch (BatchReplayException exception) {
            JsonObject failure = error(exception.failure.getCode().name(),
                exception.failure.getMessage());
            failure.add("decisionTraces", decisions(exception.traces));
            err.println(codec.toJson(failure));
            return 2;
        } catch (InputLineException exception) {
            JsonObject failure = error(exception.failure.getCode().name(),
                exception.failure.getMessage());
            failure.addProperty("input", exception.input.toString());
            failure.addProperty("line", exception.line);
            err.println(codec.toJson(failure));
            return 2;
        } catch (TemporalValidationException exception) {
            err.println(codec.toJson(error(exception.getCode().name(), exception.getMessage())));
            return 2;
        } catch (IllegalArgumentException exception) {
            err.println(codec.toJson(error("REJECT_INPUT", exception.getMessage())));
            return 2;
        } catch (IOException | RuntimeException exception) {
            err.println(codec.toJson(error("REJECT_EXECUTION", exception.getMessage())));
            return 3;
        }
    }

    private JsonObject replay(Map<String, String> options) throws IOException {
        final RetrievalProfile candidateProfile;
        final int candidateTopK;
        if (options.containsKey("candidate-profile")) {
            try {
                candidateProfile = RetrievalProfile.valueOf(options.get("candidate-profile"));
                candidateTopK = Integer.parseInt(options.getOrDefault("candidate-top-k", "20"));
            } catch (IllegalArgumentException exception) {
                throw invalid("candidate-profile must be RULE_ONLY, RULE_BM25, RULE_MINHASH or RULE_ALL; "
                    + "candidate-top-k must be a positive integer");
            }
            if (!candidateProfile.isCandidateStrategy() || candidateTopK <= 0) {
                throw invalid("candidate-profile requires a candidate strategy and positive candidate-top-k");
            }
        } else {
            if (options.containsKey("candidate-top-k")) {
                throw invalid("candidate-top-k requires candidate-profile");
            }
            candidateProfile = null;
            candidateTopK = 20;
        }
        Path input = Paths.get(required(options, "input"));
        Path output = Paths.get(required(options, "output"));
        distinct(input, output, "input and output must refer to different files");
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    events.add(codec.decodeEvent(line));
                } catch (TemporalValidationException exception) {
                    throw new InputLineException(input, lineNumber, exception);
                }
            }
        }
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        ReplayResult replay;
        try {
            replay = candidateProfile == null ? facade.replay(events)
                : facade.replay(events, candidateProfile, candidateTopK);
        } catch (TemporalValidationException exception) {
            throw new BatchReplayException(exception, facade.getDecisionTraces());
        }
        JsonObject result = json();
        result.addProperty("method", replay.getMethod());
        result.addProperty("fallbackStatus", replay.getFallbackStatus());
        JsonObject statistics = new JsonObject();
        for (Map.Entry<String, Integer> entry : replay.getStatistics().entrySet()) {
            statistics.addProperty(entry.getKey(), entry.getValue());
        }
        result.add("statistics", statistics);
        result.add("snapshot", codec.snapshotToJson(replay.getSnapshot()));
        result.add("decisionTraces", decisions(replay.getDecisionTraces()));
        store.save(output, facade);
        return result;
    }

    private JsonObject query(Map<String, String> options) throws IOException {
        Path run = Paths.get(required(options, "run"));
        QueryMode mode;
        try {
            mode = QueryMode.valueOf(required(options, "mode"));
        } catch (IllegalArgumentException exception) {
            throw invalid("mode must be CURRENT, HISTORICAL, or AS_OF");
        }
        FactKey key = new FactKey(required(options, "subject"), required(options, "predicate"),
            required(options, "scope"));
        Instant validAt = TemporalRunStore.parseTime(required(options, "valid-at"), "valid-at");
        if ((mode == QueryMode.AS_OF) != options.containsKey("system-at")) {
            throw invalid("system-at is required only for AS_OF");
        }
        Instant systemAt = mode == QueryMode.AS_OF
            ? TemporalRunStore.parseTime(options.get("system-at"), "system-at") : null;
        Path output = options.containsKey("output") ? Paths.get(options.get("output")) : null;
        if (output != null) {
            distinct(run, output, "run and query output must refer to different files");
        }
        TemporalMemoryFacade facade = store.load(run);
        QueryResult query = facade.query(mode, key, validAt, systemAt);
        JsonObject result = json();
        result.addProperty("queryId", query.getTrace().getId());
        result.addProperty("mode", query.getTrace().getMode().name());
        JsonArray versions = new JsonArray();
        for (MemoryFactVersion version : query.getVersions()) {
            versions.add(codec.versionToJson(key, version,
                facade.getSnapshot().getGeneratingEventIds().get(version.getId())));
        }
        result.add("versions", versions);
        result.add("trace", query.getTrace().toJson());
        store.save(run, facade);
        if (output != null) {
            TemporalRunStore.writeAtomic(output, codec.toJson(result));
        }
        return result;
    }

    private JsonObject trace(Map<String, String> options) throws IOException {
        String id = required(options, "id");
        TemporalMemoryFacade facade = store.load(Paths.get(required(options, "run")));
        JsonObject result = json();
        result.addProperty("id", id);
        result.add("decisionTraces", decisions(facade.getTrace(id)));
        JsonArray queries = new JsonArray();
        QueryTrace query = facade.getQueryTrace(id);
        if (query != null) {
            queries.add(query.toJson());
        }
        result.add("queryTraces", queries);
        return result;
    }

    private JsonObject candidates(Map<String, String> options) throws IOException {
        QueryMode mode;
        try {
            mode = QueryMode.valueOf(required(options, "mode"));
        } catch (IllegalArgumentException exception) {
            throw invalid("mode must be CURRENT, HISTORICAL, or AS_OF");
        }
        final FactKey key = new FactKey(required(options, "subject"), required(options, "predicate"),
            required(options, "scope"));
        final Instant validAt = TemporalRunStore.parseTime(required(options, "valid-at"), "valid-at");
        if ((mode == QueryMode.AS_OF) != options.containsKey("system-at")) {
            throw invalid("system-at is required only for AS_OF");
        }
        final Instant systemAt = mode == QueryMode.AS_OF
            ? TemporalRunStore.parseTime(options.get("system-at"), "system-at") : null;
        final String text = required(options, "text");
        int topK;
        try {
            topK = Integer.parseInt(options.getOrDefault("top-k", "20"));
        } catch (NumberFormatException exception) {
            throw invalid("top-k must be a positive integer");
        }
        if (topK <= 0) {
            throw invalid("top-k must be a positive integer");
        }
        RetrievalProfile profile;
        try {
            profile = RetrievalProfile.valueOf(options.getOrDefault("profile", "BASELINE"));
        } catch (IllegalArgumentException exception) {
            throw invalid("profile must be BASELINE, ENGLISH, LEXICAL_FUSION, "
                + "RULE_ONLY, RULE_BM25, RULE_MINHASH, or RULE_ALL");
        }
        TemporalMemoryFacade facade = store.load(Paths.get(required(options, "run")));
        return facade.candidates(mode, key, text, validAt, systemAt, topK, profile).toJson();
    }

    private JsonObject validate(Map<String, String> options) throws IOException {
        TemporalMemoryFacade facade = store.load(Paths.get(required(options, "run")));
        ValidationResult validation = facade.validateInvariants();
        JsonObject result = json();
        result.addProperty("valid", validation.isValid());
        JsonArray violations = new JsonArray();
        for (String violation : validation.getViolations()) {
            violations.add(new JsonPrimitive(violation));
        }
        result.add("violations", violations);
        return result;
    }

    private JsonArray decisions(List<DecisionTrace> traces) {
        JsonArray result = new JsonArray();
        for (DecisionTrace trace : traces) {
            result.add(trace.toJson());
        }
        return result;
    }

    private static Map<String, String> options(String[] args, String... allowed) {
        Set<String> names = new HashSet<>(Arrays.asList(allowed));
        Map<String, String> values = new HashMap<>();
        for (int index = 1; index < args.length; index += 2) {
            String option = args[index];
            if (option == null || !option.startsWith("--") || !names.contains(option.substring(2))) {
                throw invalid("Unknown option: " + option);
            }
            if (index + 1 >= args.length || args[index + 1] == null
                || args[index + 1].trim().isEmpty() || args[index + 1].startsWith("--")) {
                throw invalid("Missing value for " + option);
            }
            if (values.put(option.substring(2), args[index + 1]) != null) {
                throw invalid("Duplicate option: " + option);
            }
        }
        return values;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null) {
            throw invalid("Missing required option: --" + name);
        }
        return value;
    }

    private static void distinct(Path first, Path second, String message) throws IOException {
        if (first.toAbsolutePath().normalize().equals(second.toAbsolutePath().normalize())
            || (Files.exists(first) && Files.exists(second) && Files.isSameFile(first, second))) {
            throw invalid(message);
        }
    }

    private static JsonObject json() {
        JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        return result;
    }

    private static JsonObject error(String code, String message) {
        JsonObject result = json();
        result.addProperty("code", code);
        result.addProperty("message", message);
        return result;
    }

    private static TemporalValidationException invalid(String message) {
        return new TemporalValidationException(TemporalErrorCode.REJECT_INPUT, message);
    }

    private static final class InputLineException extends RuntimeException {

        private final Path input;
        private final int line;
        private final TemporalValidationException failure;

        private InputLineException(Path input, int line, TemporalValidationException failure) {
            super(failure);
            this.input = input;
            this.line = line;
            this.failure = failure;
        }
    }

    private static final class BatchReplayException extends RuntimeException {

        private final TemporalValidationException failure;
        private final List<DecisionTrace> traces;

        private BatchReplayException(TemporalValidationException failure, List<DecisionTrace> traces) {
            super(failure);
            this.failure = failure;
            this.traces = traces;
        }
    }
}
