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
package org.apache.geaflow.ai.retrieval.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.channel.graph.AssociationScore;
import org.apache.geaflow.ai.retrieval.channel.graph.EdgeStrength;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

/**
 * Pins that association-score components and their versions are emitted in build metadata and
 * survive a metadata round trip.
 *
 * <p>The design requires this so a published graph is scored the same way on every replay, and a
 * ranking change can be blamed on a configuration change rather than on different inputs.
 */
class AssociationScoreMetadataTest {

    @Test
    void graphMetadataRoundTripsAssociationScoreComponents() {
        Map<String, Double> priorities = new LinkedHashMap<>();
        priorities.put("studies", 1.4D);
        priorities.put("mentions", 0.7D);
        AssociationScoreMetadata score = new AssociationScoreMetadata(0.75D, 0.2D, 1.1D, 0.9D,
            1.3D, priorities, "relation-prior-v2", "association-score-v2");
        GraphBuildMetadata metadata = new GraphBuildMetadata(new GraphVersion("g", "v1"),
            "file:///graph.bin", "offline-graph", "offline-graph-v2", true)
            .withAssociationScoreMetadata(score);

        ImportMetadata original = new ImportMetadata(manifest(), "importer-1", metadata,
            Collections.emptyList(), new QualityCounters(1, 1, 2, 1, 0, 0), ImportState.READY,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", null, null, 1L, 2L);
        String json = MetadataJson.toJson(original);
        ImportMetadata restored = MetadataJson.fromJson(json, ImportMetadata.class);

        AssociationScoreMetadata actual = restored.getGraph().getAssociationScoreMetadata();
        assertNotNull(actual, "association score must survive the metadata round trip");
        assertEquals(0.75D, actual.getHopDecay(), 1.0e-12);
        assertEquals(0.2D, actual.getDegreePenalty(), 1.0e-12);
        assertEquals(1.1D, actual.getAnchorScoreWeight(), 1.0e-12);
        assertEquals(0.9D, actual.getEdgeStrengthWeight(), 1.0e-12);
        assertEquals(1.3D, actual.getSourceRelevanceWeight(), 1.0e-12);
        assertEquals(priorities, actual.getRelationPriorities());
        assertEquals("relation-prior-v2", actual.getEdgeStrengthVersion());
        assertEquals("association-score-v2", actual.getScoreVersion());
    }

    @Test
    void legacyMetadataWithoutComponentsStaysNullRatherThanDefaulted() {
        GraphBuildMetadata metadata = new GraphBuildMetadata(new GraphVersion("g", "v1"),
            "file:///graph.bin", "offline-graph", "offline-graph-v1", true);

        ImportMetadata original = new ImportMetadata(manifest(), "importer-1", metadata,
            Collections.emptyList(), new QualityCounters(1, 1, 0, 0, 0, 0), ImportState.READY,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", null, null, 1L, 2L);
        String json = MetadataJson.toJson(original);
        ImportMetadata restored = MetadataJson.fromJson(json, ImportMetadata.class);

        assertNull(restored.getGraph().getAssociationScoreMetadata(),
            "absent components must stay absent so a replay can tell unrecorded from default");
        assertTrue(json.contains("\"associationScore\":null") || !json.contains("associationScore"));
    }

    @Test
    void defaultsCarryTheDocumentedFormulaConstants() {
        AssociationScoreMetadata defaults = AssociationScoreMetadata.defaults();

        assertEquals(0.8D, defaults.getHopDecay(), 1.0e-12);
        // The documented formula has no exponent on the degree penalty, so the default exponent is
        // 1.0 and reproduces 1 / log(2 + degree) exactly.
        assertEquals(1.0D, defaults.getDegreePenalty(), 1.0e-12);
        assertEquals(AssociationScore.SCORE_VERSION, defaults.getScoreVersion());
        assertEquals(EdgeStrength.EDGE_STRENGTH_VERSION, defaults.getEdgeStrengthVersion());
    }

    @Test
    void defaultsRecordTheTimeDecayComponentAndKeepItDisabled() {
        AssociationScoreMetadata defaults = AssociationScoreMetadata.defaults();

        assertEquals("time-decay-v1", defaults.getTimeDecayVersion());
        assertEquals(EdgeStrength.DEFAULT_TIME_DECAY_WEIGHT, defaults.getTimeDecayWeight(), 1.0e-12);
        assertEquals(EdgeStrength.DEFAULT_TIME_DECAY_HALF_LIFE_MILLIS,
            defaults.getTimeDecayHalfLifeMillis(), 1.0e-12);
    }

    @Test
    void timeDecayComponentsRoundTripThroughTheMetadata() {
        AssociationScoreMetadata score = new AssociationScoreMetadata(0.75D, 1.0D, 1.0D, 1.0D, 1.0D,
            Collections.emptyMap(), "relation-prior-v2", "association-score-v2",
            "time-decay-v1", 0.5D, 43_200_000.0D);
        GraphBuildMetadata metadata = new GraphBuildMetadata(new GraphVersion("g", "v1"),
            "file:///graph.bin", "offline-graph", "offline-graph-v2", true)
            .withAssociationScoreMetadata(score);

        ImportMetadata original = new ImportMetadata(manifest(), "importer-1", metadata,
            Collections.emptyList(), new QualityCounters(1, 1, 2, 1, 0, 0), ImportState.READY,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", null, null, 1L, 2L);
        ImportMetadata restored = MetadataJson.fromJson(MetadataJson.toJson(original),
            ImportMetadata.class);

        AssociationScoreMetadata actual = restored.getGraph().getAssociationScoreMetadata();
        assertNotNull(actual);
        assertEquals("time-decay-v1", actual.getTimeDecayVersion());
        assertEquals(0.5D, actual.getTimeDecayWeight(), 1.0e-12);
        assertEquals(43_200_000.0D, actual.getTimeDecayHalfLifeMillis(), 1.0e-12);
    }

    @Test
    void legacyArtifactWithoutTimeDecayDecodesAsDisabledRatherThanFailing() {
        // Artifacts published before the time-decay component existed carry only the older fields.
        GraphBuildMetadata metadata = new GraphBuildMetadata(new GraphVersion("g", "v1"),
            "file:///graph.bin", "offline-graph", "offline-graph-v1", true);
        JsonObject associationScore = new JsonObject();
        associationScore.addProperty("hopDecay", 0.8D);
        associationScore.addProperty("degreePenalty", 0.1D);
        associationScore.addProperty("anchorScoreWeight", 1.0D);
        associationScore.addProperty("edgeStrengthWeight", 1.0D);
        associationScore.addProperty("sourceRelevanceWeight", 1.0D);
        associationScore.add("relationPriorities", new JsonObject());
        associationScore.addProperty("edgeStrengthVersion", "relation-prior-v1");
        associationScore.addProperty("scoreVersion", "association-score-v1");
        JsonObject root = new JsonParser().parse(MetadataJson.toJson(metadata)).getAsJsonObject();
        root.add("associationScoreMetadata", associationScore);

        GraphBuildMetadata decoded = MetadataJson.fromJson(root.toString(), GraphBuildMetadata.class);

        AssociationScoreMetadata actual = decoded.getAssociationScoreMetadata();
        assertNotNull(actual);
        assertEquals("association-score-v1", actual.getScoreVersion());
        assertNull(actual.getTimeDecayVersion(), "legacy artifacts must not invent a decay version");
        assertEquals(0.0D, actual.getTimeDecayWeight(), 1.0e-12);
        assertEquals(0.0D, actual.getTimeDecayHalfLifeMillis(), 1.0e-12);
    }

    @Test
    void rejectsOutOfRangeTimeDecayComponents() {
        assertThrows(MetadataException.class, () -> new AssociationScoreMetadata(0.8D, 1.0D, 1.0D,
            1.0D, 1.0D, Collections.emptyMap(), "v", "v", "time-decay-v1", 1.5D, 1.0D));
        assertThrows(MetadataException.class, () -> new AssociationScoreMetadata(0.8D, 1.0D, 1.0D,
            1.0D, 1.0D, Collections.emptyMap(), "v", "v", "time-decay-v1", -0.1D, 1.0D));
        assertThrows(MetadataException.class, () -> new AssociationScoreMetadata(0.8D, 1.0D, 1.0D,
            1.0D, 1.0D, Collections.emptyMap(), "v", "v", "time-decay-v1", 0.5D, -1.0D));
    }

    @Test
    void rejectsOutOfRangeComponents() {
        assertThrows(MetadataException.class, () -> new AssociationScoreMetadata(1.5D, 0.1D, 1.0D,
            1.0D, 1.0D, Collections.emptyMap(), "v", "v"));
        assertThrows(MetadataException.class, () -> new AssociationScoreMetadata(0.8D, -0.1D, 1.0D,
            1.0D, 1.0D, Collections.emptyMap(), "v", "v"));
    }

    private static DatasetManifest manifest() {
        return new DatasetManifest("v1", "dataset", "revision", "dev", null, "cache",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "policy",
            new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "offline", "v1", 1L);
    }
}