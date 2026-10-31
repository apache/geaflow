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

package org.apache.geaflow.ai.retrieval.model.graph;

import com.google.gson.annotations.SerializedName;
import java.util.Collections;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Immutable graph entity reference with optional aliases and source chunk provenance. */
public final class EntityRef {

    @SerializedName("entityId")
    private final String entityId;
    @SerializedName("canonicalName")
    private final String canonicalName;
    @SerializedName("aliases")
    private final java.util.List<String> aliases;
    @SerializedName("type")
    private final String type;
    @SerializedName("sourceChunkIds")
    private final java.util.List<String> sourceChunkIds;

    private EntityRef() {
        this.entityId = null;
        this.canonicalName = null;
        this.aliases = java.util.Collections.emptyList();
        this.type = null;
        this.sourceChunkIds = java.util.Collections.emptyList();
    }

    public EntityRef(String entityId, String canonicalName, String type) {
        this(entityId, canonicalName, java.util.Collections.emptyList(), type,
            java.util.Collections.emptyList());
    }

    public EntityRef(String entityId, String canonicalName, java.util.List<String> aliases,
                     String type, java.util.List<String> sourceChunkIds) {
        this.entityId = ModelValidation.required(entityId, "entityId");
        this.canonicalName = ModelValidation.required(canonicalName, "canonicalName");
        this.aliases = ModelValidation.sortedStrings(aliases, "alias");
        this.type = ModelValidation.required(type, "type");
        this.sourceChunkIds = ModelValidation.sortedStrings(sourceChunkIds, "sourceChunkId");
    }

    public String getEntityId() {
        return entityId;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    public java.util.List<String> getAliases() {
        return Collections.unmodifiableList(aliases == null ? Collections.emptyList() : aliases);
    }

    public String getType() {
        return type;
    }

    public java.util.List<String> getSourceChunkIds() {
        return Collections.unmodifiableList(sourceChunkIds == null
            ? Collections.emptyList() : sourceChunkIds);
    }

    public boolean sameIdentityAs(EntityRef other) {
        return other != null && Objects.equals(entityId, other.entityId)
            && Objects.equals(canonicalName, other.canonicalName)
            && Objects.equals(type, other.type);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof EntityRef)) {
            return false;
        }
        EntityRef that = (EntityRef) object;
        return Objects.equals(entityId, that.entityId)
            && Objects.equals(canonicalName, that.canonicalName)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(type, that.type)
            && Objects.equals(sourceChunkIds, that.sourceChunkIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId, canonicalName, aliases, type, sourceChunkIds);
    }
}
