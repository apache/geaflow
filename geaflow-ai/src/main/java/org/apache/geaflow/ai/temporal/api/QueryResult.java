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

package org.apache.geaflow.ai.temporal.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.trace.QueryTrace;

/** Query matches and the explanation generated for this request. */
public final class QueryResult {

    private final List<MemoryFactVersion> versions;
    private final QueryTrace trace;

    QueryResult(List<MemoryFactVersion> versions, QueryTrace trace) {
        this.versions = Collections.unmodifiableList(new ArrayList<>(versions));
        this.trace = trace;
    }

    public List<MemoryFactVersion> getVersions() {
        return versions;
    }

    public QueryTrace getTrace() {
        return trace;
    }
}
