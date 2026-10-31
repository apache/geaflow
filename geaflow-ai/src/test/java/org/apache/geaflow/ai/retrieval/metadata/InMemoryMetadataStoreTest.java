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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Lifecycle, readiness, CAS, and retry tests for the in-memory metadata store. */
public class InMemoryMetadataStoreTest {

    private static final String SOURCE_SHA256 = "41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d";

    @Test
    public void mismatchFailsAttemptAndRetryKeepsTheFailureRecord() throws Exception {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        GraphVersion failedVersion = new GraphVersion("graph", "g1");
        store.begin(manifest(), "importer-v1", graph(failedVersion, false, Collections.emptyList()));

        MetadataException mismatch = Assertions.assertThrows(MetadataException.class,
            () -> store.verifySource(failedVersion, input("wrong")));
        Assertions.assertEquals(MetadataException.Code.CHECKSUM_MISMATCH, mismatch.getCode());
        Assertions.assertEquals(ImportState.FAILED, store.find(failedVersion).get().getState());

        GraphVersion retryVersion = new GraphVersion("graph", "g2");
        ImportMetadata retry = store.retry(failedVersion,
            graph(retryVersion, false, Collections.emptyList()));
        Assertions.assertEquals(ImportState.IMPORTING, retry.getState());
        Assertions.assertEquals(ImportState.FAILED, store.find(failedVersion).get().getState());
        Assertions.assertEquals(2, store.findDataset("dataset", "release", "dev").size());
    }

    @Test
    public void publicationRequiresEveryRequiredIndexAndUsesCompareAndSet() throws Exception {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        GraphVersion version = new GraphVersion("graph", "g1");
        store.begin(manifest(), "importer-v1", graph(version, false,
            Collections.singletonList("keyword")));
        store.verifySource(version, input("source"));
        store.finishImport(version, graph(version, true, Collections.singletonList("keyword")),
            new QualityCounters(1, 1, 1, 0, 0, 0));

        MetadataException notReady = Assertions.assertThrows(MetadataException.class,
            () -> store.publish(version, null));
        Assertions.assertEquals(MetadataException.Code.NOT_READY, notReady.getCode());
        Assertions.assertFalse(store.published("graph").isPresent());

        IndexVersion indexVersion = new IndexVersion("keyword", "i1", "g1");
        store.putIndex(version, new IndexBuildMetadata(version, indexVersion, "bm25",
            "builder-v1", null, false));
        Assertions.assertThrows(MetadataException.class, () -> store.publish(version, null));
        store.putIndex(version, new IndexBuildMetadata(version, indexVersion, "bm25",
            "builder-v1", "memory:i1", true));
        ImportMetadata published = store.publish(version, null);
        Assertions.assertEquals(ImportState.READY, published.getState());
        Assertions.assertEquals(version, store.getPublishedVersion("graph").get());

        GraphVersion nextVersion = new GraphVersion("graph", "g2");
        store.begin(manifest(), "importer-v1", graph(nextVersion, false, Collections.emptyList()));
        store.verifySource(nextVersion, input("source"));
        store.finishImport(nextVersion, graph(nextVersion, true, Collections.emptyList()),
            new QualityCounters(1, 1, 1, 0, 0, 0));
        MetadataException conflict = Assertions.assertThrows(MetadataException.class,
            () -> store.publish(nextVersion, new GraphVersion("graph", "stale")));
        Assertions.assertEquals(MetadataException.Code.VERSION_CONFLICT, conflict.getCode());
        Assertions.assertEquals(version, store.getPublishedVersion("graph").get());
    }

    @Test
    public void completedIndexCannotBeReplaced() throws Exception {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        GraphVersion version = new GraphVersion("graph", "g1");
        store.begin(manifest(), "importer-v1", graph(version, false,
            Collections.singletonList("keyword")));
        store.verifySource(version, input("source"));
        store.finishImport(version, graph(version, true, Collections.singletonList("keyword")),
            new QualityCounters(1, 1, 1, 0, 0, 0));
        IndexVersion indexVersion = new IndexVersion("keyword", "i1", "g1");
        store.putIndex(version, new IndexBuildMetadata(version, indexVersion, "bm25",
            "builder-v1", "memory:i1", true));

        MetadataException conflict = Assertions.assertThrows(MetadataException.class,
            () -> store.putIndex(version, new IndexBuildMetadata(version,
                new IndexVersion("keyword", "i2", "g1"), "bm25", "builder-v2", "memory:i2", true)));
        Assertions.assertEquals(MetadataException.Code.VERSION_CONFLICT, conflict.getCode());
    }

    private static DatasetManifest manifest() {
        return new DatasetManifest("v1", "dataset", "release", "dev", "uri", null,
            SOURCE_SHA256, "preprocess-v1", new ChunkingConfiguration("chunk-v1", 100, 10),
            "schema-v1", null, null, 7);
    }

    private static GraphBuildMetadata graph(GraphVersion version, boolean ready,
                                            java.util.List<String> requiredIndexes) {
        return new GraphBuildMetadata(version, ready ? "memory:" + version.getVersion() : null,
            "memory", "builder-v1", ready, requiredIndexes);
    }

    private static ByteArrayInputStream input(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
