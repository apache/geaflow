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

package org.apache.geaflow.ai.temporal.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.candidate.EvidenceCandidate;
import org.apache.geaflow.ai.temporal.candidate.FactCandidate;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;

public final class CandidateResult {

    private final QueryMode mode;
    private final FactKey factKey;
    private final String text;
    private final Instant validAt;
    private final Instant systemAt;
    private final int topK;
    private final List<FactCandidate> facts;
    private final List<EvidenceCandidate> evidence;
    private final Map<String, String> generatingEventIds;
    private final RetrievalProfile profile;

    CandidateResult(QueryMode mode, FactKey factKey, String text, Instant validAt,
                    Instant systemAt, int topK, List<FactCandidate> facts,
                    List<EvidenceCandidate> evidence, Map<String, String> generatingEventIds,
                    RetrievalProfile profile) {
        this.mode = mode;
        this.factKey = factKey;
        this.text = text;
        this.validAt = validAt;
        this.systemAt = systemAt;
        this.topK = topK;
        this.facts = Collections.unmodifiableList(new ArrayList<>(facts));
        this.evidence = Collections.unmodifiableList(new ArrayList<>(evidence));
        this.generatingEventIds = Collections.unmodifiableMap(new TreeMap<>(generatingEventIds));
        this.profile = profile;
    }

    public List<FactCandidate> getFacts() {
        return facts;
    }

    public List<EvidenceCandidate> getEvidence() {
        return evidence;
    }

    public JsonObject toJson() {
        TemporalJsonCodec codec = new TemporalJsonCodec();
        JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        result.addProperty("mode", mode.name());
        result.add("factKey", codec.factKeyToJson(factKey));
        result.addProperty("validAt", validAt.toString());
        result.addProperty("systemAt", systemAt == null ? null : systemAt.toString());
        result.addProperty("text", text);
        result.addProperty("topK", topK);
        result.addProperty("retrievalProfile", profile.name());
        JsonArray encodedFacts = new JsonArray();
        for (FactCandidate candidate : facts) {
            JsonObject value = new JsonObject();
            value.addProperty("rank", candidate.getRank());
            value.addProperty("mandatory", candidate.isMandatory());
            JsonObject scores = new JsonObject();
            for (Map.Entry<String, Double> score : candidate.getScores().entrySet()) {
                scores.addProperty(score.getKey(), score.getValue());
            }
            value.add("scores", scores);
            if (profile.isCandidateStrategy()) {
                value.addProperty("score", candidate.getScore());
                JsonObject ranks = new JsonObject();
                for (Map.Entry<String, Integer> rank : candidate.getChannelRanks().entrySet()) {
                    ranks.addProperty(rank.getKey(), rank.getValue());
                }
                value.add("channelRanks", ranks);
            }
            value.add("version", codec.versionToJson(candidate.getFactKey(),
                candidate.getVersion(), generatingEventIds.get(candidate.getVersion().getId())));
            encodedFacts.add(value);
        }
        result.add("facts", encodedFacts);
        JsonArray encodedEvidence = new JsonArray();
        for (EvidenceCandidate candidate : evidence) {
            JsonObject value = new JsonObject();
            value.addProperty("rank", candidate.getRank());
            JsonObject scores = new JsonObject();
            for (Map.Entry<String, Double> score : candidate.getChannelScores().entrySet()) {
                scores.addProperty(score.getKey(), score.getValue());
            }
            value.add("scores", scores);
            if (profile.usesLexicalFusion() || profile.isCandidateStrategy()) {
                value.addProperty("score", candidate.getScore());
                JsonObject ranks = new JsonObject();
                for (Map.Entry<String, Integer> rank : candidate.getChannelRanks().entrySet()) {
                    ranks.addProperty(rank.getKey(), rank.getValue());
                }
                value.add("channelRanks", ranks);
            }
            value.add("evidence", codec.evidenceToJson(candidate.getEvidence()));
            JsonArray versionIds = new JsonArray();
            for (String id : candidate.getVersionIds()) {
                versionIds.add(new JsonPrimitive(id));
            }
            value.add("versionIds", versionIds);
            encodedEvidence.add(value);
        }
        result.add("evidence", encodedEvidence);
        return result;
    }
}
