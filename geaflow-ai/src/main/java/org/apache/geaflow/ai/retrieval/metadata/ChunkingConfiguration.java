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

/** Reproducible character-based chunking policy. */
public final class ChunkingConfiguration {

    private final String policyVersion;
    private final int chunkSize;
    private final int overlap;

    public ChunkingConfiguration(
        String policyVersion,
        int chunkSize,
        int overlap) {
        MetadataValidation.required(policyVersion, "policyVersion");
        if (chunkSize <= 0 || overlap < 0 || overlap >= chunkSize) {
            throw MetadataValidation.invalid("chunkSize must be positive and 0 <= overlap < chunkSize");
        }
        this.policyVersion = policyVersion;
        this.chunkSize = chunkSize;
        this.overlap = overlap;
    }

    public String getPolicyVersion() {
        return policyVersion;
    }

    public String getStrategyVersion() {
        return policyVersion;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getOverlap() {
        return overlap;
    }

    public int getOverlapSize() {
        return overlap;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ChunkingConfiguration)) {
            return false;
        }
        ChunkingConfiguration that = (ChunkingConfiguration) object;
        return chunkSize == that.chunkSize
            && overlap == that.overlap
            && Objects.equals(policyVersion, that.policyVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyVersion, chunkSize, overlap);
    }
}
