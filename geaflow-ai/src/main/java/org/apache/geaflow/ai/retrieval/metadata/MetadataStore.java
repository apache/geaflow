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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/**
 * Storage-neutral build lifecycle. Published snapshots are immutable.
 *
 * <p>Implementations must serialize mutations per attempt and atomically compare and replace the
 * published graph pointer. Source streams remain owned by the caller. Retry uses a new graph
 * version, preserving the failed attempt and any previously published version.</p>
 */
public interface MetadataStore {

    ImportMetadata begin(DatasetManifest manifest, String importerVersion, GraphBuildMetadata graph);

    ImportMetadata retry(GraphVersion failedVersion, GraphBuildMetadata newGraph);

    ImportMetadata verifySource(GraphVersion version, InputStream source) throws IOException;

    ImportMetadata finishImport(GraphVersion version, GraphBuildMetadata graph, QualityCounters counters);

    ImportMetadata putIndex(GraphVersion version, IndexBuildMetadata index);

    ImportMetadata fail(GraphVersion version, MetadataException.Code code, String reason);

    ImportMetadata publish(GraphVersion version, GraphVersion expectedPublishedVersion);

    Optional<ImportMetadata> find(GraphVersion version);

    Optional<ImportMetadata> published(String graphName);

    List<ImportMetadata> findDataset(String dataset, String release, String split);

    default List<ImportMetadata> findByDataset(String dataset, String release, String split) {
        return findDataset(dataset, release, split);
    }

    /** Alias for implementations and callers that use get terminology. */
    default Optional<ImportMetadata> get(GraphVersion version) {
        return find(version);
    }

    /** Returns the published snapshot for a graph name, if one exists. */
    default Optional<ImportMetadata> getPublished(String graphName) {
        return published(graphName);
    }

    /** Reads only the published graph version while retaining snapshot immutability. */
    default Optional<GraphVersion> getPublishedVersion(String graphName) {
        Optional<ImportMetadata> snapshot = published(graphName);
        return snapshot.map(ImportMetadata::getGraphVersion);
    }

    default Optional<GraphVersion> publishedVersion(String graphName) {
        return getPublishedVersion(graphName);
    }

    default Optional<GraphVersion> readPublishedVersion(String graphName) {
        return getPublishedVersion(graphName);
    }

    /** Alias for stores that expose a find-style publication query. */
    default Optional<ImportMetadata> findPublished(String graphName) {
        return published(graphName);
    }
}
