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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;

/** A committed snapshot and the decisions for this submitted finite batch. */
public final class ReplayResult {

    private final CanonicalSnapshot snapshot;
    private final List<DecisionTrace> traces;
    private final Map<String, Integer> statistics;
    private final String method;

    ReplayResult(CanonicalSnapshot snapshot, List<DecisionTrace> traces,
                 Map<String, Integer> statistics, String method) {
        this.snapshot = snapshot;
        this.traces = Collections.unmodifiableList(new ArrayList<>(traces));
        this.statistics = Collections.unmodifiableMap(new LinkedHashMap<>(statistics));
        this.method = method;
    }

    public CanonicalSnapshot getSnapshot() {
        return snapshot;
    }

    public List<DecisionTrace> getDecisionTraces() {
        return traces;
    }

    public Map<String, Integer> getStatistics() {
        return statistics;
    }

    public String getMethod() {
        return method;
    }

    public String getFallbackStatus() {
        return "NOT_APPLICABLE";
    }
}
