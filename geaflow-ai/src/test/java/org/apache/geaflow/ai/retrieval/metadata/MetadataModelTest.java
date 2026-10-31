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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Contract tests for immutable import metadata models and their JSON boundary. */
public class MetadataModelTest {

    private static final String SHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    public void manifestValidatesRequiredFieldsAndPreservesLongSeed() {
        ChunkingConfiguration chunking = new ChunkingConfiguration("chunk-v1", 512, 64);
        DatasetManifest manifest = new DatasetManifest("v1", "hotpotqa", "2025", "dev",
            "https://example.test/data", null, SHA256.toUpperCase(), "clean-v1", chunking,
            "schema-v1", "offline-v1", "embeddings-v2", Long.MIN_VALUE);

        Assertions.assertEquals("hotpotqa", manifest.getDataset());
        Assertions.assertEquals(-9223372036854775808L, manifest.getRandomSeed());
        Assertions.assertEquals(SHA256, manifest.getSha256());
        Assertions.assertEquals(manifest,
            MetadataJson.fromJson(MetadataJson.toJson(manifest), DatasetManifest.class));
    }

    @Test
    public void manifestRejectsInvalidRequiredFieldsAndPairs() {
        ChunkingConfiguration chunking = new ChunkingConfiguration("chunk-v1", 10, 2);
        Assertions.assertThrows(MetadataException.class, () -> new DatasetManifest("v2", "d", "r",
            "dev", "uri", null, SHA256, "p", chunking, "schema", null, null, 1));
        Assertions.assertThrows(MetadataException.class, () -> new DatasetManifest("v1", "d", "r",
            "dev", null, " ", SHA256, "p", chunking, "schema", null, null, 1));
        Assertions.assertThrows(MetadataException.class, () -> new DatasetManifest("v1", "d", "r",
            "dev", "uri", null, "bad", "p", chunking, "schema", null, null, 1));
        Assertions.assertThrows(MetadataException.class, () -> new DatasetManifest("v1", "d", "r",
            "dev", "uri", null, SHA256, "p", chunking, "schema", "vectors", null, 1));
        Assertions.assertThrows(MetadataException.class, () -> new ChunkingConfiguration("v1", 2, 2));
    }

    @Test
    public void graphAndIndexMetadataValidateBindingAndGraphOnlyBuilds() {
        GraphVersion graphVersion = new GraphVersion("graph", "g1");
        GraphBuildMetadata graph = new GraphBuildMetadata(graphVersion, "memory:g1", "memory",
            "builder-v1", true, Collections.emptyList());
        Assertions.assertTrue(graph.getRequiredIndexes().isEmpty());
        Assertions.assertEquals(graph, MetadataJson.fromJson(MetadataJson.toJson(graph),
            GraphBuildMetadata.class));

        IndexVersion indexVersion = new IndexVersion("keyword", "i1", "g1");
        IndexBuildMetadata index = new IndexBuildMetadata(graphVersion, indexVersion, "bm25",
            "builder-v1", "memory:i1", true);
        Assertions.assertEquals(index, MetadataJson.fromJson(MetadataJson.toJson(index),
            IndexBuildMetadata.class));
        Assertions.assertThrows(MetadataException.class, () -> new IndexBuildMetadata(graphVersion,
            new IndexVersion("keyword", "i1", "other"), "bm25", "builder-v1", "uri", true));
        Assertions.assertThrows(MetadataException.class, () -> new GraphBuildMetadata(graphVersion,
            "uri", "memory", "builder-v1", false, Arrays.asList("keyword", "keyword")));
    }

    @Test
    public void importJsonIgnoresUnknownFieldsAndDefaultsOldIndexes() {
        DatasetManifest manifest = manifest();
        GraphBuildMetadata graph = new GraphBuildMetadata(new GraphVersion("graph", "g1"), null,
            false, Collections.emptyList());
        ImportMetadata metadata = new ImportMetadata(manifest, "importer-v1", graph,
            Collections.emptyList(), new QualityCounters(1, 2, 3, 4, 5, 6), ImportState.IMPORTING,
            null, null, null, 10, 10);
        JsonObject json = new JsonParser().parse(MetadataJson.toJson(metadata)).getAsJsonObject();
        json.remove("indexes");
        json.addProperty("futureField", "ignored");

        ImportMetadata restored = MetadataJson.fromJson(json.toString(), ImportMetadata.class);
        Assertions.assertTrue(restored.getIndexes().isEmpty());
        Assertions.assertEquals(metadata.getCounters(), restored.getCounters());
        Assertions.assertEquals(ImportState.IMPORTING, restored.getState());
    }

    @Test
    public void countersRejectNegativeValues() {
        Assertions.assertThrows(MetadataException.class,
            () -> new QualityCounters(0, 0, -1, 0, 0, 0));
    }

    private static DatasetManifest manifest() {
        return new DatasetManifest("v1", "dataset", "release", "dev", "uri", null, SHA256,
            "preprocess-v1", new ChunkingConfiguration("chunk-v1", 100, 10), "schema-v1",
            null, null, 7);
    }
}
