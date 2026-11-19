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

package org.apache.geaflow.ai.temporal.benchmark;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.QueryResult;
import org.apache.geaflow.ai.temporal.api.ReplayResult;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.SnapshotComparator;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.apache.geaflow.ai.temporal.trace.QueryTrace;

/** Runs hand-authored fact Gold through every production candidate strategy. */
public final class FactGoldEvaluation {

    private static final TemporalJsonCodec CODEC = new TemporalJsonCodec();
    private static final int TOP_K = 1;
    private static final List<String> FIXTURES = Arrays.asList(
        "01-assert-entity-reference", "02-identical-duplicate", "03-event-id-reuse",
        "04-evidence-merge", "05-conflict-coexistence", "06-adjacent-intervals",
        "07-partial-correction", "08-partial-retraction", "09-valid-time-late",
        "10-system-time-disorder", "11-same-timestamp", "12-invalid-target-atomicity");
    private static final List<RetrievalProfile> PROFILES = Arrays.asList(
        RetrievalProfile.RULE_ONLY, RetrievalProfile.RULE_BM25,
        RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL);
    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);
    private static final Comparator<Instant> END_ORDER =
        Comparator.nullsLast(Comparator.naturalOrder());
    private static final Comparator<MemoryFactVersion> QUERY_ORDER =
        Comparator.comparing((MemoryFactVersion v) -> v.getValidTime().getStart())
            .thenComparing(v -> v.getValidTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(v -> v.getTransactionTime().getStart())
            .thenComparing(v -> v.getTransactionTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(MemoryFactVersion::getId);
    private static final List<String> CATEGORIES = Arrays.asList(
        "semantic", "graph", "query", "decision", "oracle", "rollback");
    private static final List<String> CSV_FIELDS = Arrays.asList(
        "profile", "caseCount", "passedCaseCount", "conflictTP", "conflictFP", "conflictFN",
        "conflictPrecision", "conflictRecall", "conflictF1", "negativeCaseCount",
        "correctNegativeCaseCount", "semanticPassedCount", "graphPassedCount",
        "queryPassedCount", "decisionPassedCount", "oraclePassedCount", "duplicateCount",
        "rejectedBatchCount", "rollbackCheckCount");

    public JsonObject evaluate(Path goldDirectory, Path outputDirectory) throws IOException {
        require(Files.isDirectory(goldDirectory), "Gold directory does not exist: " + goldDirectory);
        final Map<String, JsonObject> fixtures = new TreeMap<>();
        final JsonObject hashes = new JsonObject();
        for (String name : FIXTURES) {
            final byte[] bytes = Files.readAllBytes(goldDirectory.resolve(name + ".json"));
            final JsonObject fixture = CODEC.parseObject(new String(bytes, StandardCharsets.UTF_8));
            require(fixture.get("schemaVersion").getAsInt() == 2
                && name.equals(text(fixture, "name")), "Unexpected Gold fixture: " + name);
            fixtures.put(name, fixture);
            hashes.addProperty(name + ".json", ClimateFeverDataset.sha256(bytes));
        }
        if (Files.exists(outputDirectory)) {
            try (Stream<Path> entries = Files.list(outputDirectory)) {
                require(!entries.findAny().isPresent(), "Output directory must be empty");
            }
        } else {
            Files.createDirectories(outputDirectory);
        }
        final JsonArray cases = new JsonArray();
        final JsonArray summaries = new JsonArray();
        boolean passed = true;
        for (RetrievalProfile profile : PROFILES) {
            final List<JsonObject> profileCases = new ArrayList<>();
            for (Map.Entry<String, JsonObject> fixture : fixtures.entrySet()) {
                final Path diagnostics = outputDirectory.resolve("diagnostics")
                    .resolve(profile.name()).resolve(fixture.getKey());
                Files.createDirectories(diagnostics);
                final CaseRun run = new CaseRun(fixture.getValue(), profile);
                run.execute(diagnostics);
                cases.add(run.result);
                profileCases.add(run.result);
                passed &= run.result.get("passed").getAsBoolean();
            }
            summaries.add(aggregate(profile, profileCases));
        }
        writeCsv(outputDirectory.resolve("comparison.csv"), CSV_FIELDS, summaries);
        final List<String> caseFields = new ArrayList<>(CSV_FIELDS);
        caseFields.add(1, "fixture");
        caseFields.add("passed");
        caseFields.add("firstError");
        writeCsv(outputDirectory.resolve("cases.csv"), caseFields, cases);
        final JsonObject manifest = new JsonObject();
        manifest.addProperty("schema", "fact-gold-v1");
        manifest.addProperty("fixtureCount", fixtures.size());
        manifest.addProperty("caseCount", cases.size());
        manifest.addProperty("topK", TOP_K);
        manifest.addProperty("passed", passed);
        manifest.addProperty("goldSource", "hand-authored fixtures; Oracle is a separate check");
        manifest.addProperty("conflictUnit", "final materialized canonical CONFLICTS_WITH edge");
        manifest.addProperty("negativeCasePolicy", "empty Gold and prediction: null P/R/F1; "
            + "count separately as a correct negative; aggregate uses summed TP/FP/FN");
        manifest.addProperty("zeroDenominatorPolicy", "undefined precision/recall/F1 is null");
        manifest.addProperty("batchPolicy", "fixture endExclusive boundaries; atomic applyBatch");
        manifest.add("inputSha256", hashes);
        manifest.add("profiles", summaries);
        manifest.add("cases", cases);
        final JsonObject outputs = new JsonObject();
        for (String file : Arrays.asList("comparison.csv", "cases.csv")) {
            outputs.addProperty(file, ClimateFeverDataset.sha256(
                Files.readAllBytes(outputDirectory.resolve(file))));
        }
        manifest.add("outputSha256", outputs);
        writeJson(outputDirectory.resolve("manifest.json"), manifest);
        return manifest;
    }

    static JsonObject conflictMetrics(List<VersionRelation> gold,
                                      List<VersionRelation> predicted) {
        final Set<VersionRelation> expected = conflicts(gold);
        final Set<VersionRelation> actual = conflicts(predicted);
        final Set<VersionRelation> hits = new HashSet<>(expected);
        hits.retainAll(actual);
        return metrics(hits.size(), actual.size() - hits.size(), expected.size() - hits.size());
    }

    private static Set<VersionRelation> conflicts(List<VersionRelation> relations) {
        final Set<VersionRelation> result = new HashSet<>();
        for (VersionRelation relation : relations) {
            if (relation.getType() == VersionRelationType.CONFLICTS_WITH) {
                result.add(relation);
            }
        }
        return result;
    }

    private static JsonObject metrics(int tp, int fp, int fn) {
        final JsonObject result = new JsonObject();
        result.addProperty("conflictTP", tp);
        result.addProperty("conflictFP", fp);
        result.addProperty("conflictFN", fn);
        result.add("conflictPrecision", ratio(tp, tp + fp));
        result.add("conflictRecall", ratio(tp, tp + fn));
        result.add("conflictF1", ratio(2 * tp, 2 * tp + fp + fn));
        return result;
    }

    private static JsonElement ratio(int numerator, int denominator) {
        return denominator == 0 ? JsonNull.INSTANCE
            : new JsonPrimitive((double) numerator / denominator);
    }

    private static JsonObject aggregate(RetrievalProfile profile, List<JsonObject> cases) {
        final JsonObject result = new JsonObject();
        result.addProperty("profile", profile.name());
        for (String field : CSV_FIELDS.subList(1, CSV_FIELDS.size())) {
            if ("conflictPrecision".equals(field) || "conflictRecall".equals(field)
                || "conflictF1".equals(field)) {
                continue;
            }
            int total = 0;
            for (JsonObject item : cases) {
                total += item.get(field).getAsInt();
            }
            result.addProperty(field, total);
        }
        copy(metrics(result.get("conflictTP").getAsInt(), result.get("conflictFP").getAsInt(),
            result.get("conflictFN").getAsInt()), result);
        return result;
    }

    private static final class CaseRun {

        private final JsonObject fixture;
        private final RetrievalProfile profile;
        private final Map<String, JsonObject> eventSpecs = new HashMap<>();
        private final Map<String, JsonObject> definitions = new HashMap<>();
        private final Map<String, NormalizedMemoryEvent> events = new HashMap<>();
        private final Map<String, String> identities = new HashMap<>();
        private final Set<String> resolving = new HashSet<>();
        private final TemporalMemoryFacade facade = new TemporalMemoryFacade();
        private final List<NormalizedMemoryEvent> committed = new ArrayList<>();
        private final JsonObject result = new JsonObject();
        private final JsonArray failures = new JsonArray();
        private final Map<String, Boolean> checks = new TreeMap<>();
        private CanonicalSnapshot expected;
        private int duplicates;
        private int rejections;
        private int rollbackChecks;

        private CaseRun(JsonObject fixture, RetrievalProfile profile) {
            this.fixture = fixture;
            this.profile = profile;
            result.addProperty("profile", profile.name());
            result.addProperty("fixture", text(fixture, "name"));
            for (JsonElement item : fixture.getAsJsonArray("events")) {
                final JsonObject spec = item.getAsJsonObject();
                require(eventSpecs.put(text(spec, "alias"), spec) == null, "Duplicate input alias");
            }
            for (String field : Arrays.asList("identities", "versions")) {
                for (JsonElement item : fixture.getAsJsonObject("expected").getAsJsonArray(field)) {
                    final JsonObject spec = item.getAsJsonObject();
                    require(definitions.put(text(spec, "alias"), spec) == null,
                        "Duplicate version alias");
                }
            }
            for (String alias : definitions.keySet()) {
                identity(alias);
                version(definitions.get(alias));
            }
            for (String alias : eventSpecs.keySet()) {
                normalize(alias);
            }
            expectedRelations();
            for (JsonElement item : fixture.getAsJsonObject("expected").getAsJsonArray("queries")) {
                for (JsonElement alias : item.getAsJsonObject().getAsJsonArray("versions")) {
                    require(definitions.containsKey(alias.getAsString()),
                        "Unknown Gold query version alias: " + alias.getAsString());
                }
            }
        }

        private void execute(Path output) throws IOException {
            try {
                batches();
                expected = expectedSnapshot();
                check("semantic", new SnapshotComparator().compare(expected, facade.getSnapshot())
                    .isEquivalent(), "Final snapshot differs from hand-authored Gold");
                check("oracle", new SnapshotComparator().compare(expected,
                    new FullReplayOracle().replayToSnapshot(committed)).isEquivalent(),
                    "Independent Oracle differs from hand-authored Gold");
                final MemoryGraphAdapter adapter = new MemoryGraphAdapter();
                check("graph", new SnapshotComparator().compare(expected,
                    adapter.fromGraph(adapter.toGraph(facade.getSnapshot()))).isEquivalent(),
                    "Graph round trip differs from hand-authored Gold");
                check("graph", facade.validateInvariants().isValid(), "Facade invariants failed");
                queries();
                traces();
            } catch (RuntimeException error) {
                check("semantic", false, error.getClass().getSimpleName() + ": " + error.getMessage());
            }
            copy(conflictMetrics(expectedRelations(), facade.getSnapshot().getState().getRelations()),
                result);
            final boolean negative = result.get("conflictTP").getAsInt()
                + result.get("conflictFN").getAsInt() == 0;
            result.addProperty("negativeCaseCount", negative ? 1 : 0);
            result.addProperty("correctNegativeCaseCount", negative
                && result.get("conflictFP").getAsInt() == 0 ? 1 : 0);
            result.addProperty("caseCount", 1);
            result.addProperty("passed", failures.size() == 0);
            result.addProperty("passedCaseCount", failures.size() == 0 ? 1 : 0);
            for (String category : CATEGORIES) {
                result.addProperty(category + "PassedCount",
                    Boolean.TRUE.equals(checks.get(category)) ? 1 : 0);
            }
            result.addProperty("duplicateCount", duplicates);
            result.addProperty("rejectedBatchCount", rejections);
            result.addProperty("rollbackCheckCount", rollbackChecks);
            result.addProperty("firstError", failures.size() == 0
                ? null : failures.get(0).getAsString());
            result.add("failures", failures);
            writeJson(output.resolve("checks.json"), result);
            writeJson(output.resolve("actual-snapshot.json"), CODEC.snapshotToJson(facade.getSnapshot()));
            if (expected != null) {
                writeJson(output.resolve("expected-snapshot.json"), CODEC.snapshotToJson(expected));
            }
            final List<JsonObject> decisionTraces = new ArrayList<>();
            for (DecisionTrace trace : facade.getDecisionTraces()) {
                decisionTraces.add(trace.toJson());
            }
            writeLines(output.resolve("decision-traces.jsonl"), decisionTraces);
            final List<JsonObject> queryTraces = new ArrayList<>();
            for (QueryTrace trace : facade.getQueryTraces()) {
                queryTraces.add(trace.toJson());
            }
            writeLines(output.resolve("query-traces.jsonl"), queryTraces);
        }

        private void batches() {
            int start = 0;
            for (JsonElement item : fixture.getAsJsonArray("batches")) {
                final JsonObject batch = item.getAsJsonObject();
                final int end = batch.get("endExclusive").getAsInt();
                require(end > start && end <= fixture.getAsJsonArray("events").size(),
                    "Invalid batch boundary");
                final List<NormalizedMemoryEvent> attempted = new ArrayList<>();
                for (int index = start; index < end; index++) {
                    attempted.add(normalize(text(fixture.getAsJsonArray("events")
                        .get(index).getAsJsonObject(), "alias")));
                }
                attempted.sort(EVENT_ORDER);
                final CanonicalSnapshot before = facade.getSnapshot();
                final int traceCount = facade.getDecisionTraces().size();
                final String expectedFailure = nullable(batch, "expectedFailure");
                String actualFailure = null;
                ReplayResult replay = null;
                try {
                    replay = facade.applyBatch(attempted, profile, TOP_K);
                } catch (TemporalValidationException error) {
                    actualFailure = error.getCode().name();
                }
                check("decision", java.util.Objects.equals(expectedFailure, actualFailure),
                    "Batch " + end + " expected " + expectedFailure + ", got " + actualFailure);
                if (expectedFailure == null) {
                    committed.addAll(attempted);
                    check("decision", replay != null, "Expected committed batch at " + end);
                    if (replay != null) {
                        batchDecisions(batch, replay, attempted);
                    }
                } else {
                    rejections++;
                    rollbackChecks++;
                    check("rollback", before.equals(facade.getSnapshot()),
                        "Rejected batch published state at " + end);
                    final List<DecisionTrace> traces = facade.getDecisionTraces();
                    check("decision", traces.size() == traceCount + attempted.size(),
                        "Rejected batch trace count mismatch at " + end);
                    for (DecisionTrace trace : traces.subList(traceCount, traces.size())) {
                        final JsonObject json = trace.toJson();
                        check("decision", "BATCH_ROLLED_BACK".equals(text(json, "code"))
                            && expectedFailure.equals(text(json, "validationCode"))
                            && strings(json, "generatedVersionIds").isEmpty(),
                            "Rejected attempt trace has publication or wrong error at " + end);
                    }
                    final List<NormalizedMemoryEvent> invalid = new ArrayList<>(committed);
                    invalid.addAll(attempted);
                    String oracleFailure = null;
                    try {
                        new FullReplayOracle().replayNormalized(invalid);
                    } catch (TemporalValidationException error) {
                        oracleFailure = error.getCode().name();
                    }
                    check("oracle", expectedFailure.equals(oracleFailure),
                        "Independent Oracle rejection mismatch at " + end);
                }
                check("oracle", new FullReplayOracle().replayNormalized(committed)
                    .equals(facade.getSnapshot().getState()), "Oracle prefix mismatch at " + end);
                start = end;
            }
            require(start == fixture.getAsJsonArray("events").size(), "Unconsumed input events");
        }

        private void batchDecisions(JsonObject batch, ReplayResult replay,
                                    List<NormalizedMemoryEvent> attempted) {
            final JsonArray specs = batch.getAsJsonArray("decisions");
            final List<DecisionTrace> traces = replay.getDecisionTraces();
            check("decision", specs.size() == traces.size(), "Batch decision count mismatch");
            for (int index = 0; index < Math.min(specs.size(), traces.size()); index++) {
                final JsonObject spec = specs.get(index).getAsJsonObject();
                final JsonObject trace = traces.get(index).toJson();
                final String code = text(spec, "code");
                check("decision", normalize(text(spec, "event")).equals(attempted.get(index))
                    && normalize(text(spec, "event")).getEventId().equals(text(trace, "eventId"))
                    && code.equals(text(trace, "code")), "Batch decision/order mismatch");
                if ("DUPLICATE_NOOP".equals(code)) {
                    duplicates++;
                    check("decision", "NOT_EVALUATED_DUPLICATE".equals(text(trace, "retrievalStatus"))
                        && strings(trace, "generatedVersionIds").isEmpty(),
                        "Duplicate attempt performed retrieval/publication");
                }
            }
        }

        private void traces() {
            final Map<String, JsonObject> accepted = new HashMap<>();
            final Set<String> attemptIds = new HashSet<>();
            for (DecisionTrace trace : facade.getDecisionTraces()) {
                final JsonObject json = trace.toJson();
                check("decision", attemptIds.add(trace.getId()), "Duplicate trace attempt ID");
                check("decision", profile.name().equals(text(json, "retrievalProfile"))
                    && json.get("candidateTopK").getAsInt() == TOP_K,
                    "Trace lost candidate strategy or topK");
                if ("ACCEPTED".equals(text(json, "code"))) {
                    accepted.put(trace.getEventId(), json);
                }
            }
            for (NormalizedMemoryEvent event : facade.getSnapshot().getEvents()) {
                final JsonObject trace = accepted.get(event.getEventId());
                check("decision", trace != null, "Missing accepted trace: " + event.getEventId());
                if (trace != null) {
                    check("decision", event.getPayloadHash().equals(text(trace, "payloadHash"))
                        && !strings(trace, "rules").isEmpty()
                        && "COMPLETED".equals(text(trace, "retrievalStatus")),
                        "Accepted trace lost rule inputs or completed retrieval");
                    if (event.getTargetVersionId().isPresent()) {
                        check("decision", event.getTargetVersionId().get()
                            .equals(text(trace, "targetVersionId"))
                            && strings(trace, "readVersionIds")
                                .contains(event.getTargetVersionId().get()),
                            "Targeted trace did not read the exact target");
                    }
                }
            }
        }

        private void queries() {
            final Set<String> modes = new HashSet<>();
            for (JsonElement item : fixture.getAsJsonObject("expected").getAsJsonArray("queries")) {
                final JsonObject spec = item.getAsJsonObject();
                final QueryMode mode = QueryMode.valueOf(text(spec, "mode"));
                modes.add(mode.name());
                final FactKey key = new FactKey(text(spec, "subjectId"),
                    text(spec, "predicate"), text(spec, "scope"));
                final List<MemoryFactVersion> hits = new ArrayList<>();
                for (JsonElement alias : spec.getAsJsonArray("versions")) {
                    hits.add(version(definitions.get(alias.getAsString())));
                }
                hits.sort(QUERY_ORDER);
                final QueryResult actual = facade.query(mode, key, Instant.parse(text(spec, "validAt")),
                    nullable(spec, "systemAt") == null ? null : Instant.parse(text(spec, "systemAt")));
                check("query", hits.equals(actual.getVersions()), "Query hits/order differ: " + spec);
                check("query", actual.getTrace().toJson().equals(
                    facade.getQueryTrace(actual.getTrace().getId()).toJson()), "Query trace lookup differs");
                final Set<String> expectedIds = new TreeSet<>();
                for (MemoryFactVersion version : hits) {
                    expectedIds.add(version.getId());
                }
                check("query", expectedIds.equals(strings(actual.getTrace().toJson(), "hitVersionIds")),
                    "Query trace hit IDs differ from Gold");
            }
            check("query", modes.containsAll(Arrays.asList("CURRENT", "HISTORICAL", "AS_OF")),
                "Fixture does not cover all query modes");
        }

        private CanonicalSnapshot expectedSnapshot() {
            final Map<FactKey, List<MemoryFactVersion>> byKey = new TreeMap<>();
            final Map<String, String> provenance = new TreeMap<>();
            for (JsonElement item : fixture.getAsJsonObject("expected").getAsJsonArray("versions")) {
                final JsonObject spec = item.getAsJsonObject();
                final MemoryFactVersion version = version(spec);
                final FactKey key = new FactKey(text(spec.getAsJsonObject("subject"), "id"),
                    text(spec, "predicate"), text(spec, "scope"));
                byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(version);
                provenance.put(version.getId(), normalize(text(spec, "event")).getEventId());
            }
            return new CanonicalSnapshot(new TemporalState(byKey, expectedRelations()),
                committed, provenance);
        }

        private List<VersionRelation> expectedRelations() {
            final List<VersionRelation> result = new ArrayList<>();
            for (JsonElement item : fixture.getAsJsonObject("expected").getAsJsonArray("relations")) {
                final JsonObject spec = item.getAsJsonObject();
                result.add(new VersionRelation(VersionRelationType.valueOf(text(spec, "type")),
                    identity(text(spec, "from")), identity(text(spec, "to"))));
            }
            return result;
        }

        private NormalizedMemoryEvent normalize(String alias) {
            if (events.containsKey(alias)) {
                return events.get(alias);
            }
            final JsonObject spec = eventSpecs.get(alias);
            require(spec != null && resolving.add("event:" + alias), "Unknown/cyclic event alias: " + alias);
            final JsonObject json = new JsonObject();
            json.addProperty("schemaVersion", 2);
            for (String field : Arrays.asList("id", "operation", "subject", "predicate", "scope",
                "factId", "value", "validFrom", "validTo", "recordedAt", "evidence")) {
                json.add(field, spec.has(field) ? spec.get(field) : JsonNull.INSTANCE);
            }
            final String target = nullable(spec, "target");
            json.addProperty("target", definitions.containsKey(target) ? identity(target) : target);
            final NormalizedMemoryEvent result = CODEC.decodeEvent(json);
            events.put(alias, result);
            resolving.remove("event:" + alias);
            return result;
        }

        private String identity(String alias) {
            if (identities.containsKey(alias)) {
                return identities.get(alias);
            }
            final JsonObject spec = definitions.get(alias);
            require(spec != null && resolving.add("version:" + alias), "Unknown/cyclic version alias: " + alias);
            final String source = nullable(spec, "sourceVersion");
            final String result = VersionIdentity.create(normalize(text(spec, "event")), fact(spec),
                MemoryFactVersionStatus.valueOf(text(spec, "status")), interval(spec, "valid"),
                source == null ? null : identity(source));
            identities.put(alias, result);
            resolving.remove("version:" + alias);
            return result;
        }

        private MemoryFactVersion version(JsonObject spec) {
            require(spec != null, "Unknown Gold query version alias");
            return new MemoryFactVersion(identity(text(spec, "alias")), fact(spec),
                MemoryFactVersionStatus.valueOf(text(spec, "status")), interval(spec, "valid"),
                interval(spec, "system"), evidence(spec.getAsJsonArray("evidence")));
        }

        private void check(String category, boolean condition, String message) {
            checks.put(category, checks.getOrDefault(category, true) && condition);
            if (!condition) {
                failures.add(new JsonPrimitive(category + ": " + message));
            }
        }
    }

    private static MemoryFact fact(JsonObject spec) {
        final JsonObject subject = spec.getAsJsonObject("subject");
        final MemoryEntity entity = new MemoryEntity(text(subject, "id"), text(subject, "label"));
        final JsonObject value = spec.getAsJsonObject("value");
        if ("ENTITY_REF".equals(text(value, "kind"))) {
            return MemoryFact.relationship(text(spec, "factId"), entity, text(spec, "predicate"),
                new MemoryEntity(text(value, "value"), text(value, "label")));
        }
        require("LITERAL".equals(text(value, "kind")), "Invalid Gold value kind");
        return MemoryFact.attribute(text(spec, "factId"), entity, text(spec, "predicate"),
            text(value, "value"));
    }

    private static List<Evidence> evidence(JsonArray specs) {
        final List<Evidence> result = new ArrayList<>();
        for (JsonElement item : specs) {
            final JsonObject spec = item.getAsJsonObject();
            final JsonObject source = spec.getAsJsonObject("source");
            result.add(new Evidence(text(spec, "id"), new Source(text(source, "id"),
                text(source, "name")), text(spec, "content")));
        }
        return result;
    }

    private static TimeInterval interval(JsonObject spec, String prefix) {
        final String end = nullable(spec, prefix + "To");
        return new TimeInterval(Instant.parse(text(spec, prefix + "From")),
            end == null ? null : Instant.parse(end));
    }

    private static Set<String> strings(JsonObject json, String field) {
        final Set<String> result = new TreeSet<>();
        for (JsonElement value : json.getAsJsonArray(field)) {
            require(result.add(value.getAsString()), "Duplicate trace reference in " + field);
        }
        return result;
    }

    private static String text(JsonObject json, String field) {
        final String value = nullable(json, field);
        require(value != null && !value.trim().isEmpty(), "Missing Gold field: " + field);
        return value;
    }

    private static String nullable(JsonObject json, String field) {
        return !json.has(field) || json.get(field).isJsonNull() ? null : json.get(field).getAsString();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void copy(JsonObject source, JsonObject target) {
        for (Map.Entry<String, JsonElement> item : source.entrySet()) {
            target.add(item.getKey(), item.getValue());
        }
    }

    private static void writeJson(Path path, JsonObject value) throws IOException {
        Files.write(path, (CODEC.toJson(value) + "\n").getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE_NEW);
    }

    private static void writeLines(Path path, List<JsonObject> values) throws IOException {
        final List<String> lines = new ArrayList<>();
        for (JsonObject value : values) {
            lines.add(CODEC.toJson(value));
        }
        Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private static void writeCsv(Path path, List<String> fields, JsonArray rows) throws IOException {
        final List<String> lines = new ArrayList<>();
        lines.add(String.join(",", fields));
        for (JsonElement row : rows) {
            final JsonObject json = row.getAsJsonObject();
            final List<String> values = new ArrayList<>();
            for (String field : fields) {
                final String value = nullable(json, field);
                values.add(value == null ? "" : "\"" + value.replace("\"", "\"\"") + "\"");
            }
            lines.add(String.join(",", values));
        }
        Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }
}
