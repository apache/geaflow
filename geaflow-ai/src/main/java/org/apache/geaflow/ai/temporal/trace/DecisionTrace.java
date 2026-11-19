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

/** Immutable event-attempt or replay explanation in the versioned JSON protocol. */
public final class DecisionTrace {

    private final String json;
    private final String id;
    private final String eventId;
    private final String code;

    public DecisionTrace(JsonObject value) {
        Objects.requireNonNull(value, "value");
        id = value.get("attemptId").getAsString();
        eventId = value.get("eventId").isJsonNull()
            ? null : value.get("eventId").getAsString();
        code = value.get("code").getAsString();
        json = value.toString();
    }

    public String getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getCode() {
        return code;
    }

    public JsonObject toJson() {
        return new JsonParser().parse(json).getAsJsonObject();
    }
}
