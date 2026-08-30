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
 * An immutable piece of evidence and its source.
 */
public final class Evidence {

    private final String id;
    private final Source source;
    private final String content;

    public Evidence(String id, Source source, String content) {
        this.id = requireText(id, "id");
        this.source = Objects.requireNonNull(source, "source");
        this.content = requireText(content, "content");
    }

    public String getId() {
        return id;
    }

    public Source getSource() {
        return source;
    }

    public String getContent() {
        return content;
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Evidence " + fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Evidence)) {
            return false;
        }
        Evidence that = (Evidence) object;
        return id.equals(that.id)
            && source.equals(that.source)
            && content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, content);
    }
}
