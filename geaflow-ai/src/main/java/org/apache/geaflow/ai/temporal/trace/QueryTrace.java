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

package org.apache.geaflow.ai.temporal.trace;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.api.QueryMode;

/** Immutable query explanation; evidence comes only from the selected versions. */
public final class QueryTrace {

    private final String json;
    private final String id;
    private final QueryMode mode;

    public QueryTrace(JsonObject value) {
        Objects.requireNonNull(value, "value");
        id = value.get("queryId").getAsString();
        mode = QueryMode.valueOf(value.get("mode").getAsString());
        json = value.toString();
    }

    public String getId() {
        return id;
    }

    public QueryMode getMode() {
        return mode;
    }

    public JsonObject toJson() {
        return new JsonParser().parse(json).getAsJsonObject();
    }
}
