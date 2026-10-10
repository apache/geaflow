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

package org.apache.geaflow.ai.retrieval.ingest;

import java.util.Objects;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.metadata.QualityCounters;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Immutable build inputs plus the counters collected by the pipeline. */
public final class IngestionContext {

    private final DatasetManifest manifest;
    private final GraphVersion graphVersion;
    private final String importerVersion;
    private volatile QualityCounters qualityCounters = new QualityCounters(0, 0, 0, 0, 0, 0);

    public IngestionContext(DatasetManifest manifest, GraphVersion graphVersion,
                            String importerVersion) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.graphVersion = Objects.requireNonNull(graphVersion, "graphVersion");
        this.importerVersion = Objects.requireNonNull(importerVersion, "importerVersion");
        if (importerVersion.trim().isEmpty()) {
            throw new IllegalArgumentException("importerVersion must not be blank");
        }
    }

    public DatasetManifest getManifest() {
        return manifest;
    }

    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    public String getImporterVersion() {
        return importerVersion;
    }

    public long getRandomSeed() {
        return manifest.getRandomSeed();
    }

    public QualityCounters getQualityCounters() {
        return qualityCounters;
    }

    void setQualityCounters(QualityCounters qualityCounters) {
        this.qualityCounters = Objects.requireNonNull(qualityCounters, "qualityCounters");
    }
}
