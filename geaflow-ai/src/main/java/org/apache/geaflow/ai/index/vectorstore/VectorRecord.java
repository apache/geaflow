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

import java.util.Collections;
import java.util.Map;

public class VectorRecord {
    private final String vectorId;
    private final double[] embedding;
    private final String sourceType;
    private final String sourceId;
    private final Map<String, String> metadata;

    public VectorRecord(String vectorId, double[] embedding, String sourceType, String sourceId, Map<String, String> metadata) {
        if (vectorId == null || vectorId.isEmpty()) {
            throw new IllegalArgumentException("vectorId cannot be null or empty");
        }
        if (embedding == null || embedding.length == 0) {
            throw new IllegalArgumentException("embedding cannot be null or empty");
        }
        if (sourceType == null || sourceType.isEmpty()) {
            throw new IllegalArgumentException("sourceType cannot be null or empty");
        }
        if (sourceId == null || sourceId.isEmpty()) {
            throw new IllegalArgumentException("sourceId cannot be null or empty");
        }
        this.vectorId = vectorId;
        this.embedding = embedding;
        this.sourceType = sourceType;
        this.sourceId = sourceId;
        this.metadata = metadata == null ? Collections.emptyMap() : metadata;
    }

    public String getVectorId() {
        return vectorId;
    }

    public double[] getEmbedding() {
        return embedding;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSourceId() {
        return sourceId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
