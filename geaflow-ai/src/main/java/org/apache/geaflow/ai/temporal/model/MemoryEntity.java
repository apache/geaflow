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

package org.apache.geaflow.ai.temporal.model;

import java.util.Objects;

/**
 * An immutable identity anchor for temporal memory facts.
 */
public final class MemoryEntity {

    private final String id;
    private final String label;

    public MemoryEntity(String id, String label) {
        this.id = requireText(id, "id");
        this.label = requireText(label, "label");
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Memory entity " + fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MemoryEntity)) {
            return false;
        }
        MemoryEntity that = (MemoryEntity) object;
        return id.equals(that.id) && label.equals(that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label);
    }
}
