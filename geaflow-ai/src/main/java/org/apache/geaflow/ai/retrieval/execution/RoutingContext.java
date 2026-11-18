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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Immutable inputs available to a routing policy for one request. */
public final class RoutingContext {
    private final String query;
    private final RoutingFeatures features;
    private final Set<String> availableIndexes;
    private final RetrievalBudget budget;
    private final RetrievalMode requestedMode;
    private final ExecutionMode executionMode;
    private final int maxHop;
    private final int maxReplanCount;
    private final String configVersion;
    private final long seed;
    private final List<String> policyParameters;

    private RoutingContext(Builder builder) {
        query = requireText(builder.query, "query");
        features = builder.features == null ? RoutingFeatures.empty() : builder.features;
        availableIndexes = Collections.unmodifiableSet(new LinkedHashSet<>(builder.availableIndexes));
        budget = copyBudget(builder.budget);
        requestedMode = builder.requestedMode == null ? RetrievalMode.HYBRID : builder.requestedMode.canonical();
        executionMode = builder.executionMode == null ? ExecutionMode.SEQUENTIAL : builder.executionMode;
        maxHop = builder.maxHop;
        maxReplanCount = builder.maxReplanCount;
        configVersion = requireText(builder.configVersion, "configVersion");
        seed = builder.seed;
        policyParameters = Collections.unmodifiableList(new ArrayList<>(builder.policyParameters));
        if (maxHop < 0 || maxReplanCount < 0 || budget == null) {
            throw new IllegalArgumentException("invalid routing context bounds");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private static RetrievalBudget copyBudget(RetrievalBudget value) {
        if (value == null || value.getTopK() == null || value.getMaxCandidates() == null
            || value.getTimeoutMs() == null || value.getTokenBudget() == null
            || value.getPrimaryConstraint() == null) {
            throw new IllegalArgumentException("complete retrieval budget is required");
        }
        return new RetrievalBudget(value.getTopK(), value.getTimeoutMs(), value.getMaxCandidates(),
            value.getTokenBudget(), value.getPrimaryConstraint());
    }

    public String getQuery() {
        return query;
    }

    public RoutingFeatures getFeatures() {
        return features;
    }

    public Set<String> getAvailableIndexes() {
        return availableIndexes;
    }

    public boolean hasIndex(String name) {
        return name != null && availableIndexes.contains(name.trim().toUpperCase(java.util.Locale.ROOT));
    }

    public RetrievalBudget getBudget() {
        return copyBudget(budget);
    }

    public RetrievalMode getRequestedMode() {
        return requestedMode;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public int getMaxReplanCount() {
        return maxReplanCount;
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public long getSeed() {
        return seed;
    }

    public List<String> getPolicyParameters() {
        return policyParameters;
    }

    /** Builder with conservative hard bounds suitable for request-local routing. */
    public static final class Builder {
        private String query;
        private RoutingFeatures features;
        private final Set<String> availableIndexes = new LinkedHashSet<>();
        private RetrievalBudget budget;
        private RetrievalMode requestedMode = RetrievalMode.HYBRID;
        private ExecutionMode executionMode = ExecutionMode.SEQUENTIAL;
        private int maxHop = 2;
        private int maxReplanCount;
        private String configVersion = "v1";
        private long seed;
        private final List<String> policyParameters = new ArrayList<>();

        public Builder query(String value) {
            query = value;
            return this;
        }

        public Builder features(RoutingFeatures value) {
            features = value;
            return this;
        }

        public Builder availableIndexes(Iterable<String> values) {
            availableIndexes.clear();
            if (values != null) {
                for (String value : values) {
                    if (value != null && !value.trim().isEmpty()) {
                        availableIndexes.add(value.trim().toUpperCase(java.util.Locale.ROOT));
                    }
                }
            }
            return this;
        }

        public Builder addIndex(String value) {
            if (value != null && !value.trim().isEmpty()) {
                availableIndexes.add(value.trim().toUpperCase(java.util.Locale.ROOT));
            }
            return this;
        }

        public Builder budget(RetrievalBudget value) {
            budget = value;
            return this;
        }

        public Builder requestedMode(RetrievalMode value) {
            requestedMode = value;
            return this;
        }

        public Builder executionMode(ExecutionMode value) {
            executionMode = value;
            return this;
        }

        public Builder maxHop(int value) {
            maxHop = value;
            return this;
        }

        public Builder maxReplanCount(int value) {
            maxReplanCount = value;
            return this;
        }

        public Builder configVersion(String value) {
            configVersion = value;
            return this;
        }

        public Builder seed(long value) {
            seed = value;
            return this;
        }

        public Builder policyParameters(Iterable<String> values) {
            policyParameters.clear();
            if (values != null) {
                for (String value : values) {
                    if (value != null) {
                        policyParameters.add(value);
                    }
                }
            }
            return this;
        }

        public RoutingContext build() {
            return new RoutingContext(this);
        }
    }
}
