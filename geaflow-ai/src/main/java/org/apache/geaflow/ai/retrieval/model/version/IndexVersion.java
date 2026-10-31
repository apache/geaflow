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

package org.apache.geaflow.ai.retrieval.model.version;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Immutable index snapshot identifier together with its source graph version. */
public final class IndexVersion {

    @SerializedName("indexName")
    private final String indexName;
    @SerializedName("version")
    private final String version;
    @SerializedName("graphVersion")
    private final String graphVersion;

    public IndexVersion(String indexName, String version, String graphVersion) {
        this.indexName = ModelValidation.required(indexName, "indexName");
        this.version = ModelValidation.required(version, "version");
        this.graphVersion = ModelValidation.required(graphVersion, "graphVersion");
    }

    public String getIndexName() {
        return indexName;
    }

    public String getVersion() {
        return version;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public boolean sameIdentityAs(IndexVersion other) {
        return other != null && Objects.equals(indexName, other.indexName)
            && Objects.equals(version, other.version)
            && Objects.equals(graphVersion, other.graphVersion);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IndexVersion)) {
            return false;
        }
        IndexVersion that = (IndexVersion) object;
        return Objects.equals(indexName, that.indexName)
            && Objects.equals(version, that.version)
            && Objects.equals(graphVersion, that.graphVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, version, graphVersion);
    }
}
