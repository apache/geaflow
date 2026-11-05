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

import java.util.Collections;
import java.util.Map;

/** Stateless SPI for deterministic or learned retrieval routing policies. */
public interface RoutingPolicy {
    String getPolicyId();

    String getPolicyVersion();

    default Map<String, String> getParameters() {
        return Collections.emptyMap();
    }

    RetrievalPlan plan(RoutingContext context, RetrievalProgress progress);

    default RetrievalPlan plan(RoutingContext context) {
        RoutingFeatures features = context.getFeatures();
        return plan(context, RetrievalProgress.builder().boundary("INITIAL")
            .firstStageScoreGap(features.getFirstStageScoreGap())
            .anchorCount(features.getAnchorCount()).anchorConfidence(features.getAnchorConfidence())
            .candidateDegree(features.getCandidateDegree()).graphGrowthRate(features.getGraphGrowthRate())
            .build());
    }

    default boolean supports(org.apache.geaflow.ai.retrieval.api.model.RetrievalMode mode) {
        return mode != null;
    }
}
