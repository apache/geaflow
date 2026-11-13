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

import java.util.Objects;

/** Immutable inputs needed to reproduce a dataset build. */
public final class DatasetManifest {

    public static final String CURRENT_VERSION = "v1";

    private final String manifestVersion;
    private final String dataset;
    private final String datasetRelease;
    private final String split;
    private final String sourceUri;
    private final String cachePath;
    private final String sha256;
    private final String preprocessingVersion;
    private final ChunkingConfiguration chunking;
    private final String graphSchemaVersion;
    private final String vectorSource;
    private final String vectorVersion;
    private final long randomSeed;

    public DatasetManifest(
        String manifestVersion,
        String dataset,
        String datasetRelease,
        String split,
        String sourceUri,
        String cachePath,
        String sha256,
        String preprocessingVersion,
        ChunkingConfiguration chunking,
        String graphSchemaVersion,
        String vectorSource,
        String vectorVersion,
        long randomSeed) {
        if (!CURRENT_VERSION.equals(manifestVersion)) {
            throw MetadataValidation.invalid("unsupported manifestVersion");
        }
        MetadataValidation.required(dataset, "dataset");
        MetadataValidation.required(datasetRelease, "datasetRelease");
        MetadataValidation.required(split, "split");
        if ((sourceUri == null || sourceUri.trim().isEmpty()) && (cachePath == null || cachePath.trim().isEmpty())) {
            throw MetadataValidation.invalid("sourceUri or cachePath is required");
        }
        sha256 = MetadataValidation.checksum(sha256);
        MetadataValidation.required(preprocessingVersion, "preprocessingVersion");
        if (chunking == null) {
            throw MetadataValidation.invalid("chunking is required");
        }
        MetadataValidation.required(graphSchemaVersion, "graphSchemaVersion");
        if ((vectorSource == null) != (vectorVersion == null)) {
            throw MetadataValidation.invalid("vectorSource and vectorVersion must be supplied together");
        }
        if (vectorSource != null) {
            MetadataValidation.required(vectorSource, "vectorSource");
            MetadataValidation.required(vectorVersion, "vectorVersion");
        }
        this.manifestVersion = manifestVersion;
        this.dataset = dataset;
        this.datasetRelease = datasetRelease;
        this.split = split;
        this.sourceUri = sourceUri;
        this.cachePath = cachePath;
        this.sha256 = sha256;
        this.preprocessingVersion = preprocessingVersion;
        this.chunking = chunking;
        this.graphSchemaVersion = graphSchemaVersion;
        this.vectorSource = vectorSource;
        this.vectorVersion = vectorVersion;
        this.randomSeed = randomSeed;
    }

    public String getManifestVersion() {
        return manifestVersion;
    }

    public String getDataset() {
        return dataset;
    }

    public String getDatasetRelease() {
        return datasetRelease;
    }

    public String getDatasetVersion() {
        return datasetRelease;
    }

    public String getRelease() {
        return datasetRelease;
    }

    public String getSplit() {
        return split;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public String getSourceUrl() {
        return sourceUri;
    }

    public String getCachePath() {
        return cachePath;
    }

    public String getSha256() {
        return sha256;
    }

    public String getChecksum() {
        return sha256;
    }

    public String getPreprocessingVersion() {
        return preprocessingVersion;
    }

    public ChunkingConfiguration getChunking() {
        return chunking;
    }

    public String getGraphSchemaVersion() {
        return graphSchemaVersion;
    }

    public String getVectorSource() {
        return vectorSource;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    /** Returns the dataset identifier using the terminology used by ingestion clients. */
    public String getDatasetId() {
        return dataset;
    }

    /** Returns the dataset name alias retained for manifest consumers. */
    public String getDatasetName() {
        return dataset;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DatasetManifest)) {
            return false;
        }
        DatasetManifest that = (DatasetManifest) object;
        return randomSeed == that.randomSeed
            && Objects.equals(manifestVersion, that.manifestVersion)
            && Objects.equals(dataset, that.dataset)
            && Objects.equals(datasetRelease, that.datasetRelease)
            && Objects.equals(split, that.split)
            && Objects.equals(sourceUri, that.sourceUri)
            && Objects.equals(cachePath, that.cachePath)
            && Objects.equals(sha256, that.sha256)
            && Objects.equals(preprocessingVersion, that.preprocessingVersion)
            && Objects.equals(chunking, that.chunking)
            && Objects.equals(graphSchemaVersion, that.graphSchemaVersion)
            && Objects.equals(vectorSource, that.vectorSource)
            && Objects.equals(vectorVersion, that.vectorVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestVersion, dataset, datasetRelease, split, sourceUri, cachePath,
            sha256, preprocessingVersion, chunking, graphSchemaVersion, vectorSource, vectorVersion,
            randomSeed);
    }
}
