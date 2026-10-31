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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.Source;

public final class ClimateFeverDataset {

    public static final int SUBSET_SIZE = 1000;
    public static final String RELEASE_COMMIT = "336f0a46c98920a6f40926385ce71ba877a7b863";
    public static final String RELEASE_SHA256 =
        "8a4b9032d861be482ffb49dddfd283ffa6089e654f1e968040011882c5eb6e0b";
    public static final String SOURCE_URL = "https://raw.githubusercontent.com/tdiggelm/"
        + "climate-fever-dataset/" + RELEASE_COMMIT + "/dataset/climate-fever.jsonl";
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final List<Claim> claims;
    private final List<Evidence> corpus;
    private final String subsetJsonl;
    private final String corpusJsonl;
    private final String mappingsJsonl;
    private final String goldJsonl;
    private final Map<String, List<String>> titlesByEvidenceId;
    private final String titlesJsonl;
    private final JsonObject manifest;

    private ClimateFeverDataset(List<Claim> claims, List<Evidence> corpus, String subsetJsonl,
                               String corpusJsonl, String mappingsJsonl, String goldJsonl,
                               Map<String, List<String>> titlesByEvidenceId, String titlesJsonl,
                               JsonObject manifest) {
        this.claims = Collections.unmodifiableList(claims);
        this.corpus = Collections.unmodifiableList(corpus);
        this.subsetJsonl = subsetJsonl;
        this.corpusJsonl = corpusJsonl;
        this.mappingsJsonl = mappingsJsonl;
        this.goldJsonl = goldJsonl;
        this.titlesByEvidenceId = Collections.unmodifiableMap(titlesByEvidenceId);
        this.titlesJsonl = titlesJsonl;
        this.manifest = manifest;
    }

    public static ClimateFeverDataset load(Path raw, String expectedSha256) throws IOException {
        if (expectedSha256 == null || !expectedSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("Expected SHA-256 must contain exactly 64 hex digits");
        }
        byte[] bytes = Files.readAllBytes(raw);
        String actualSha256 = sha256(bytes);
        if (!actualSha256.equals(expectedSha256.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Raw dataset SHA-256 does not match: " + actualSha256);
        }
        TemporalJsonCodec codec = new TemporalJsonCodec();
        Map<BigInteger, JsonObject> records = new TreeMap<>();
        Map<String, String> rawEvidenceContents = new HashMap<>();
        int lineNumber = 0;
        for (String line : new String(bytes, StandardCharsets.UTF_8).split("\\r?\\n", -1)) {
            lineNumber++;
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonObject record;
            try {
                record = codec.parseObject(line);
                validateRecord(record, rawEvidenceContents);
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("Invalid dataset line " + lineNumber + ": "
                    + exception.getMessage(), exception);
            }
            String id = text(record, "claim_id");
            if (!id.matches("0|[1-9][0-9]*")) {
                throw new IllegalArgumentException("claim_id must be a canonical nonnegative integer: " + id);
            }
            if (records.put(new BigInteger(id), record) != null) {
                throw new IllegalArgumentException("Duplicate claim_id: " + id);
            }
        }
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Dataset must contain at least one claim");
        }

        List<Claim> claims = new ArrayList<>();
        Map<String, Evidence> corpus = new TreeMap<>();
        Map<String, Set<String>> titleSets = new TreeMap<>();
        StringBuilder subset = new StringBuilder();
        StringBuilder mappings = new StringBuilder();
        StringBuilder gold = new StringBuilder();
        int noGoldCount = 0;
        int neiCount = 0;
        int evidencePairCount = 0;
        long goldPairCount = 0;
        for (JsonObject record : records.values()) {
            if (claims.size() == SUBSET_SIZE) {
                break;
            }
            String claimId = text(record, "claim_id");
            String claimLabel = text(record, "claim_label");
            Set<String> goldIds = new TreeSet<>();
            JsonObject selected = new JsonObject();
            selected.addProperty("claim_id", claimId);
            selected.addProperty("claim", text(record, "claim"));
            selected.addProperty("claim_label", claimLabel);
            JsonArray selectedEvidence = new JsonArray();
            Map<String, JsonObject> sortedEvidence = new TreeMap<>();
            for (JsonElement item : record.getAsJsonArray("evidences")) {
                JsonObject evidence = item.getAsJsonObject();
                sortedEvidence.put(text(evidence, "evidence_id"), evidence);
            }
            for (JsonObject evidence : sortedEvidence.values()) {
                String content = text(evidence, "evidence");
                String evidenceId = "climate-evidence-" + sha256(content.getBytes(StandardCharsets.UTF_8));
                String article = text(evidence, "article");
                titleSets.computeIfAbsent(evidenceId, ignored -> new TreeSet<>()).add(article);
                String sourceId = "wikipedia-" + sha256(article.getBytes(StandardCharsets.UTF_8));
                Evidence candidate = new Evidence(evidenceId, new Source(sourceId, article), content);
                Evidence existing = corpus.get(evidenceId);
                if (existing != null && !existing.getContent().equals(content)) {
                    throw new IllegalArgumentException("Evidence SHA-256 collision: " + evidenceId);
                }
                // One text may occur in several articles; all original sources stay in mappings.
                if (existing == null || article.compareTo(existing.getSource().getName()) < 0) {
                    corpus.put(evidenceId, candidate);
                }
                String label = text(evidence, "evidence_label");
                if ("SUPPORTS".equals(label) || "REFUTES".equals(label)) {
                    goldIds.add(evidenceId);
                }
                JsonObject mapping = new JsonObject();
                mapping.addProperty("claim_id", claimId);
                mapping.addProperty("raw_evidence_id", text(evidence, "evidence_id"));
                mapping.addProperty("evidence_id", evidenceId);
                mapping.addProperty("evidence_label", label);
                mapping.addProperty("source_id", sourceId);
                mapping.addProperty("article", article);
                append(mappings, mapping);
                JsonObject entry = new JsonObject();
                entry.addProperty("raw_evidence_id", text(evidence, "evidence_id"));
                entry.addProperty("evidence_id", evidenceId);
                entry.addProperty("evidence_label", label);
                entry.addProperty("article", article);
                selectedEvidence.add(entry);
                evidencePairCount++;
            }
            selected.add("evidences", selectedEvidence);
            append(subset, selected);
            Claim claim = new Claim(claimId, text(record, "claim"), claimLabel, goldIds);
            claims.add(claim);
            JsonObject queryGold = new JsonObject();
            queryGold.addProperty("claim_id", claimId);
            queryGold.add("gold_evidence_ids", strings(goldIds));
            append(gold, queryGold);
            goldPairCount += goldIds.size();
            if (goldIds.isEmpty()) {
                noGoldCount++;
            }
            if ("NOT_ENOUGH_INFO".equals(claimLabel)) {
                neiCount++;
            }
        }
        StringBuilder corpusJsonl = new StringBuilder();
        for (Evidence evidence : corpus.values()) {
            JsonObject item = new JsonObject();
            item.addProperty("evidence_id", evidence.getId());
            item.addProperty("text", evidence.getContent());
            item.addProperty("source_id", evidence.getSource().getId());
            item.addProperty("article", evidence.getSource().getName());
            append(corpusJsonl, item);
        }
        Map<String, List<String>> titlesByEvidenceId = new TreeMap<>();
        StringBuilder titlesJsonl = new StringBuilder();
        for (Map.Entry<String, Set<String>> entry : titleSets.entrySet()) {
            titlesByEvidenceId.put(entry.getKey(),
                Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            JsonObject item = new JsonObject();
            item.addProperty("evidence_id", entry.getKey());
            item.add("titles", strings(entry.getValue()));
            append(titlesJsonl, item);
        }
        JsonObject manifest = new JsonObject();
        manifest.addProperty("processorVersion", "climate-fever-prefix-v1");
        manifest.addProperty("dataset", "CLIMATE-FEVER");
        boolean official = RELEASE_SHA256.equals(actualSha256);
        manifest.addProperty("release", official ? "1.0.1" : "custom-input");
        manifest.addProperty("officialReleaseBytes", official);
        if (official) {
            manifest.addProperty("sourceCommit", RELEASE_COMMIT);
            manifest.addProperty("sourceUrl", SOURCE_URL);
            manifest.addProperty("sourcePage", "https://github.com/tdiggelm/climate-fever-dataset/releases/tag/1.0.1");
        }
        manifest.addProperty("rawSha256", actualSha256);
        manifest.addProperty("rawBytes", bytes.length);
        manifest.addProperty("license", "unknown");
        manifest.addProperty("licenseSource", "https://huggingface.co/datasets/tdiggelm/climate_fever/blob/main/README.md");
        manifest.addProperty("licenseNote", "Author dataset card declares unknown; no dataset license in source repository");
        manifest.addProperty("attribution", "Diggelmann, Boyd-Graber, Bulian, Ciaramita, Leippold (2020), arXiv:2012.00614");
        manifest.addProperty("selection", "Ascending numeric claim_id; first min(1000, raw claim count)");
        manifest.addProperty("evaluationCorpus", "Deduplicated attached evidence of selected claims; not full Wikipedia");
        manifest.addProperty("evidenceIdentity", "Exact original UTF-8 text SHA-256; no lexical normalization");
        manifest.addProperty("sourceSelection", "Lexicographically smallest article per identical text; retain all source mappings");
        manifest.addProperty("goldDefinition", "Distinct evidence text IDs with SUPPORTS or REFUTES; NOT_ENOUGH_INFO is not positive");
        manifest.addProperty("zeroGoldPolicy", "Exclude zero-positive queries from recall denominators; report separately");
        manifest.addProperty("rawClaimCount", records.size());
        manifest.addProperty("selectedClaimCount", claims.size());
        manifest.addProperty("selectedEvidencePairCount", evidencePairCount);
        manifest.addProperty("corpusCount", corpus.size());
        manifest.addProperty("positivePairCount", goldPairCount);
        manifest.addProperty("zeroGoldClaimCount", noGoldCount);
        manifest.addProperty("notEnoughInfoClaimCount", neiCount);
        List<String> claimIds = new ArrayList<>();
        for (Claim claim : claims) {
            claimIds.add(claim.getId());
        }
        manifest.add("selectedClaimIds", strings(claimIds));
        manifest.addProperty("subsetSha256", sha256(subset.toString().getBytes(StandardCharsets.UTF_8)));
        manifest.addProperty("corpusSha256", sha256(corpusJsonl.toString().getBytes(StandardCharsets.UTF_8)));
        manifest.addProperty("mappingsSha256", sha256(mappings.toString().getBytes(StandardCharsets.UTF_8)));
        manifest.addProperty("goldSha256", sha256(gold.toString().getBytes(StandardCharsets.UTF_8)));
        manifest.addProperty("titleSelection", "All distinct original article titles per selected evidence text, regardless of label");
        manifest.addProperty("titlesSha256", sha256(titlesJsonl.toString().getBytes(StandardCharsets.UTF_8)));
        return new ClimateFeverDataset(claims, new ArrayList<>(corpus.values()), subset.toString(),
            corpusJsonl.toString(), mappings.toString(), gold.toString(),
            titlesByEvidenceId, titlesJsonl.toString(), manifest);
    }

    private static void validateRecord(JsonObject record, Map<String, String> rawEvidenceContents) {
        text(record, "claim_id");
        text(record, "claim");
        label(record, "claim_label", true);
        if (!record.has("evidences") || !record.get("evidences").isJsonArray()) {
            throw new IllegalArgumentException("evidences must be an array");
        }
        Set<String> evidenceIds = new HashSet<>();
        for (JsonElement item : record.getAsJsonArray("evidences")) {
            if (!item.isJsonObject()) {
                throw new IllegalArgumentException("Evidence must be an object");
            }
            JsonObject evidence = item.getAsJsonObject();
            String id = text(evidence, "evidence_id");
            if (!evidenceIds.add(id)) {
                throw new IllegalArgumentException("Duplicate evidence_id in one claim: " + id);
            }
            String content = text(evidence, "evidence");
            String article = text(evidence, "article");
            label(evidence, "evidence_label", false);
            String signature = GSON.toJson(Arrays.asList(article, content));
            String existing = rawEvidenceContents.put(id, signature);
            if (existing != null && !existing.equals(signature)) {
                throw new IllegalArgumentException("Conflicting content/source for evidence_id: " + id);
            }
        }
    }

    private static void label(JsonObject object, String name, boolean claim) {
        String value = text(object, name);
        if (!Arrays.asList("SUPPORTS", "REFUTES", "NOT_ENOUGH_INFO").contains(value)
            && !(claim && "DISPUTED".equals(value))) {
            throw new IllegalArgumentException("Unknown " + name + ": " + value);
        }
    }

    private static String text(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()
            || value.getAsString().trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must be a nonempty string");
        }
        return value.getAsString();
    }

    static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            char[] result = new char[digest.length * 2];
            for (int index = 0; index < digest.length; index++) {
                result[index * 2] = Character.forDigit((digest[index] & 0xff) >>> 4, 16);
                result[index * 2 + 1] = Character.forDigit(digest[index] & 0x0f, 16);
            }
            return new String(result);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static JsonArray strings(Iterable<String> values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(new com.google.gson.JsonPrimitive(value));
        }
        return array;
    }

    private static void append(StringBuilder builder, JsonObject object) {
        builder.append(GSON.toJson(object)).append('\n');
    }

    public List<Claim> getClaims() {
        return claims;
    }

    public List<Evidence> getCorpus() {
        return corpus;
    }

    public Map<String, List<String>> getTitlesByEvidenceId() {
        return titlesByEvidenceId;
    }

    public JsonObject getManifest() {
        return new com.google.gson.JsonParser().parse(GSON.toJson(manifest)).getAsJsonObject();
    }

    public void writeArtifacts(Path output) throws IOException {
        Files.write(output.resolve("subset.jsonl"), subsetJsonl.getBytes(StandardCharsets.UTF_8));
        Files.write(output.resolve("corpus.jsonl"), corpusJsonl.getBytes(StandardCharsets.UTF_8));
        Files.write(output.resolve("source-mappings.jsonl"), mappingsJsonl.getBytes(StandardCharsets.UTF_8));
        Files.write(output.resolve("gold.jsonl"), goldJsonl.getBytes(StandardCharsets.UTF_8));
        Files.write(output.resolve("titles.jsonl"), titlesJsonl.getBytes(StandardCharsets.UTF_8));
    }

    public static final class Claim {

        private final String id;
        private final String text;
        private final String label;
        private final Set<String> goldIds;

        private Claim(String id, String text, String label, Set<String> goldIds) {
            this.id = id;
            this.text = text;
            this.label = label;
            this.goldIds = Collections.unmodifiableSet(goldIds);
        }

        public String getId() {
            return id;
        }

        public String getText() {
            return text;
        }

        public String getLabel() {
            return label;
        }

        public Set<String> getGoldIds() {
            return goldIds;
        }
    }
}
