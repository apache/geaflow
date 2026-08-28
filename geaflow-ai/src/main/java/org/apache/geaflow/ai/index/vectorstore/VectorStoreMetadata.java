/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.index.vectorstore;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;

public class VectorStoreMetadata {
    @SerializedName("model_name")
    private final String modelName;
    @SerializedName("dimension")
    private final int dimension;
    @SerializedName("distance")
    private final DistanceMetric distance;
    @SerializedName("index_version")
    private final String indexVersion;
    @SerializedName("created_at")
    private final long createdAt;
    @SerializedName("format_version")
    private final int formatVersion;

    public VectorStoreMetadata(String modelName, int dimension, DistanceMetric distance,
                               String indexVersion, long createdAt, int formatVersion) {
        if (modelName == null || modelName.isEmpty()) {
            throw new IllegalArgumentException("modelName cannot be null or empty");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be greater than 0");
        }
        if (distance == null) {
            throw new IllegalArgumentException("distance cannot be null");
        }
        if (indexVersion == null || indexVersion.isEmpty()) {
            throw new IllegalArgumentException("indexVersion cannot be null or empty");
        }
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

    public int getDimension() {
        return dimension;
    }

    public DistanceMetric getDistance() {
        return distance;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public int getFormatVersion() {
        return formatVersion;
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
               && formatVersion == that.formatVersion
               && Objects.equals(modelName, that.modelName)
               && distance == that.distance
               && Objects.equals(indexVersion, that.indexVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, dimension, distance, indexVersion, createdAt, formatVersion);
    }
}
