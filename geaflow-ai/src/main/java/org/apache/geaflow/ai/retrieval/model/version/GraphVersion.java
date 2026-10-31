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

/** Immutable name/version pair identifying a graph snapshot. */
public final class GraphVersion {

    @SerializedName("graphName")
    private final String graphName;
    @SerializedName("version")
    private final String version;

    public GraphVersion(String graphName, String version) {
        this.graphName = ModelValidation.required(graphName, "graphName");
        this.version = ModelValidation.required(version, "version");
    }

    public String getGraphName() {
        return graphName;
    }

    public String getVersion() {
        return version;
    }

    public boolean sameIdentityAs(GraphVersion other) {
        return other != null && Objects.equals(graphName, other.graphName)
            && Objects.equals(version, other.version);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphVersion)) {
            return false;
        }
        GraphVersion that = (GraphVersion) object;
        return Objects.equals(graphName, that.graphName)
            && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphName, version);
    }
}
