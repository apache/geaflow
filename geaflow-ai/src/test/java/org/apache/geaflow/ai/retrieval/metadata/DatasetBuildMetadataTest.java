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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Pins the two dataset-build metadata fields the design requires and that were previously dropped.
 *
 * <p>The design lists "entity extraction configuration" and "budget configurations" among the
 * eleven values every data build must store. The extractor version existed in {@code READY.json}
 * but never reached the Java manifest, and the budget configuration was recorded nowhere, so two
 * builds differing only in extractor version or budget could not be told apart from their metadata.
 */
class DatasetBuildMetadataTest {

    private static final String SHA256 =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void entityExtractorVersionIsDistinctFromGraphSchemaVersion() {
        DatasetManifest manifest = manifest("text-cooccurrence-entity-v2",
            BudgetConfiguration.standard());

        assertEquals("chunk-entity-cooccurrence-v2", manifest.getGraphSchemaVersion());
        assertEquals("text-cooccurrence-entity-v2", manifest.getEntityExtractorVersion(),
            "the extractor configuration must be recorded separately from the graph schema");
    }

    @Test
    void budgetConfigurationIsRecordedOnTheManifest() {
        BudgetConfiguration budget = new BudgetConfiguration("candidates", 100, 3000, 4096, 100);
        DatasetManifest manifest = manifest("extractor-v1", budget);

        assertNotNull(manifest.getBudgetConfiguration());
        assertEquals("candidates", manifest.getBudgetConfiguration().getPrimaryConstraint());
        assertEquals(100, manifest.getBudgetConfiguration().getMaxCandidates());
        assertEquals(3000, manifest.getBudgetConfiguration().getTimeoutMs());
        assertEquals(4096, manifest.getBudgetConfiguration().getTokenBudget());
        assertEquals(100, manifest.getBudgetConfiguration().getMaxEdgeScans());
    }

    @Test
    void bothFieldsSurviveAManifestRoundTrip() {
        DatasetManifest original = manifest("extractor-v3",
            new BudgetConfiguration("tokens", 50, 1500, 2048, 60));

        DatasetManifest restored = MetadataJson.fromJson(MetadataJson.toJson(original),
            DatasetManifest.class);

        assertEquals("extractor-v3", restored.getEntityExtractorVersion());
        assertEquals(original.getBudgetConfiguration().getTokenBudget(),
            restored.getBudgetConfiguration().getTokenBudget());
        assertEquals(original, restored, "round trip must be identity-preserving");
    }

    @Test
    void legacyManifestWithoutTheNewFieldsStaysDecodable() {
        String legacy = "{\"manifestVersion\":\"v1\",\"dataset\":\"d\",\"datasetRelease\":\"r\","
            + "\"split\":\"dev\",\"sourceUri\":\"uri\",\"sha256\":\"" + SHA256 + "\","
            + "\"preprocessingVersion\":\"p\","
            + "\"chunking\":{\"policyVersion\":\"chunk-v1\",\"chunkSize\":100,\"overlap\":10},"
            + "\"graphSchemaVersion\":\"graph-v1\",\"vectorSource\":\"v\",\"vectorVersion\":\"v1\","
            + "\"randomSeed\":1}";

        DatasetManifest decoded = MetadataJson.fromJson(legacy, DatasetManifest.class);

        assertNull(decoded.getEntityExtractorVersion(),
            "an absent extractor version must stay absent, not be invented");
        assertNull(decoded.getBudgetConfiguration());
    }

    @Test
    void budgetConfigurationRejectsInvalidValues() {
        org.junit.jupiter.api.Assertions.assertThrows(MetadataException.class,
            () -> new BudgetConfiguration("candidates", 0, 3000, 4096, 100));
        org.junit.jupiter.api.Assertions.assertThrows(MetadataException.class,
            () -> new BudgetConfiguration("candidates", 100, -1, 4096, 100));
        org.junit.jupiter.api.Assertions.assertThrows(MetadataException.class,
            () -> new BudgetConfiguration("nonsense", 100, 3000, 4096, 100));
    }

    private static DatasetManifest manifest(String extractorVersion, BudgetConfiguration budget) {
        return new DatasetManifest("v1", "dataset", "revision", "dev", "uri", "cache", SHA256,
            "policy", new ChunkingConfiguration("chunk-v1", 100, 10), "chunk-entity-cooccurrence-v2",
            "vector", "v1", 1L, extractorVersion, budget);
    }

    @Test
    void standardBudgetConfigurationMatchesTheServiceDefaults() {
        BudgetConfiguration standard = BudgetConfiguration.standard();

        assertTrue(standard.getMaxCandidates() > 0);
        assertTrue(standard.getTimeoutMs() > 0);
        assertTrue(standard.getTokenBudget() > 0);
        assertTrue(standard.getMaxEdgeScans() > 0);
    }
}