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

package org.apache.geaflow.ai.index.vectorstore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class VectorStoreMetadata {
    
    @SerializedName("model_name")
    private String modelName;
    
    @SerializedName("dimension")
    private int dimension;
    
    @SerializedName("distance")
    private String distance;
    
    @SerializedName("index_version")
    private String indexVersion;
    
    @SerializedName("created_at")
    private long createdAt;
    
    @SerializedName("format_version")
    private String formatVersion;

    public VectorStoreMetadata() {
    }

    public VectorStoreMetadata(String modelName, int dimension, String distance, 
                               String indexVersion, long createdAt, String formatVersion) {
        this.modelName = modelName;
        this.dimension = dimension;
        this.distance = distance;
        this.indexVersion = indexVersion;
        this.createdAt = createdAt;
        this.formatVersion = formatVersion;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public void setIndexVersion(String indexVersion) {
        this.indexVersion = indexVersion;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public String getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(String formatVersion) {
        this.formatVersion = formatVersion;
    }

    public void validate(VectorStoreMetadata expected) {
        if (expected == null) {
            return;
        }
        if (this.dimension != expected.dimension) {
            throw new IllegalArgumentException(String.format(
                "Dimension mismatch. Expected %d, but got %d", expected.dimension, this.dimension));
        }
        if (!Objects.equals(this.modelName, expected.modelName)) {
            throw new IllegalArgumentException(String.format(
                "Model mismatch. Expected %s, but got %s", expected.modelName, this.modelName));
        }
    }

    public void validateComplete() {
        if (modelName == null || modelName.isEmpty()) {
            throw new IllegalArgumentException("Missing metadata: model_name");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("Missing or invalid metadata: dimension");
        }
        if (distance == null || distance.isEmpty()) {
            throw new IllegalArgumentException("Missing metadata: distance");
        }
        if (indexVersion == null || indexVersion.isEmpty()) {
            throw new IllegalArgumentException("Missing metadata: index_version");
        }
        if (formatVersion == null || formatVersion.isEmpty()) {
            throw new IllegalArgumentException("Missing metadata: format_version");
        }
    }

    public static VectorStoreMetadata load(Path metadataPath) throws IOException {
        if (!Files.exists(metadataPath)) {
            throw new IOException("Metadata file does not exist: " + metadataPath.toAbsolutePath());
        }
        try (BufferedReader reader = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            Gson gson = new Gson();
            VectorStoreMetadata metadata = gson.fromJson(reader, VectorStoreMetadata.class);
            if (metadata == null) {
                throw new IOException("Failed to parse metadata from file");
            }
            metadata.validateComplete();
            return metadata;
        }
    }

    public void save(Path metadataPath) throws IOException {
        validateComplete();
        try (BufferedWriter writer = Files.newBufferedWriter(metadataPath, StandardCharsets.UTF_8)) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(this, writer);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorStoreMetadata that = (VectorStoreMetadata) o;
        return dimension == that.dimension
               && createdAt == that.createdAt
               && Objects.equals(modelName, that.modelName)
               && Objects.equals(distance, that.distance)
               && Objects.equals(indexVersion, that.indexVersion)
               && Objects.equals(formatVersion, that.formatVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, dimension, distance, indexVersion, createdAt, formatVersion);
    }
}
