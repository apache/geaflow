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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Configuration defaults and hard-limit tests. */
public class RetrievalPropertiesTest {

    @Test
    public void validatesVersionedDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertEquals("v1", properties.getConfigVersion());
        Assertions.assertEquals("Confucius", properties.getReadyGraphName());
        Assertions.assertEquals("KEYWORD", properties.getDefaultMode());
        Assertions.assertEquals("SEQUENTIAL", properties.getDefaultExecutionMode());
        Assertions.assertEquals(10, properties.getDefaultTopK());
        Assertions.assertEquals(100, properties.getMaxTopK());
        Assertions.assertEquals(3000, properties.getDefaultTimeoutMs());
        Assertions.assertEquals(10000, properties.getMaxTimeoutMs());
        Assertions.assertEquals(100, properties.getDefaultMaxCandidates());
        Assertions.assertEquals(1000, properties.getMaxCandidates());
        Assertions.assertEquals(4096, properties.getDefaultTokenBudget());
        Assertions.assertEquals(16384, properties.getMaxTokenBudget());
    }

    @Test
    public void exposesAdaptiveRoutingDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertEquals("deterministic-rule", properties.getRoutingPolicyId());
        Assertions.assertEquals(1, properties.getMaxReplanCount());
        Assertions.assertTrue(properties.isStrictPlanValidation());
        Assertions.assertEquals("deterministic-reranker-v1", properties.getRerankerId());
        Assertions.assertEquals("chars4-v1", properties.getTokenEstimatorId());
        Assertions.assertEquals(1.5D, properties.getHighAverageIdf(), 1.0e-12);
        Assertions.assertEquals(0.20D, properties.getHighScoreGap(), 1.0e-12);
        Assertions.assertEquals(12, properties.getLongQueryLength());
        Assertions.assertEquals(0.75D, properties.getStrongAnchorConfidence(), 1.0e-12);
        Assertions.assertEquals(1, properties.getMinimumAnchorCount());
        Assertions.assertEquals(32, properties.getHighCandidateDegree());
        Assertions.assertEquals(2.0D, properties.getHighGrowthRate(), 1.0e-12);
        Assertions.assertEquals(16, properties.getBaseNeighborCap());
    }

    @Test
    public void rejectsUnsupportedAdaptiveRoutingConfiguration() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setRoutingPolicyId("learned-policy");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMaxReplanCount(5);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMaxReplanCount(-1);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setRerankerId("unknown");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setTokenEstimatorId("unknown");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setHighAverageIdf(Double.NaN);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setHighScoreGap(-0.1D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setLongQueryLength(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setStrongAnchorConfidence(1.1D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMinimumAnchorCount(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setHighCandidateDegree(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setHighGrowthRate(-1.0D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setBaseNeighborCap(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMaxReplanCount(4);
        properties.validateConfiguration();
    }

    @Test
    public void rejectsInvalidModesAndLimits() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setDefaultMode("UNKNOWN");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultMode("HYBRID");
        properties.validateConfiguration();

        properties = new RetrievalProperties();
        properties.setDefaultExecutionMode("PARALLEL");
        properties.validateConfiguration();

        properties = new RetrievalProperties();
        properties.setDefaultExecutionMode("CASCADED");
        properties.validateConfiguration();

        properties = new RetrievalProperties();
        properties.setDefaultTimeoutMs(10001);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultTokenBudget(16385);
        assertInvalid(properties);
    }

    @Test
    public void validatesGraphExpansionBoundaries() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setGraphSampleRate(0.5D);
        properties.setGraphMinMarginalYield(0.1D);
        properties.setGraphMinAssociationScore(0.2D);
        properties.setGraphMinAssociationSurvivalRate(0.3D);
        properties.validateConfiguration();

        properties = new RetrievalProperties();
        properties.setGraphSampleRate(1.5D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setGraphSampleRate(-0.1D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setGraphMinAssociationScore(1.5D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setGraphMinMarginalYield(-1.0D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setGraphMinAssociationSurvivalRate(1.5D);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultMaxEdgeScans(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultMaxEdgeScans(100);
        properties.setMaxEdgeScans(10);
        assertInvalid(properties);
    }

    @Test
    public void rejectsBlankIdentityAndInvertedDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setConfigVersion(" ");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setReadyGraphName(null);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultTopK(10);
        properties.setDefaultMaxCandidates(9);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMaxTopK(0);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setConfigVersion("v2");
        assertInvalid(properties);
    }

    private static void assertInvalid(RetrievalProperties properties) {
        RetrievalException exception = Assertions.assertThrows(RetrievalException.class,
            properties::validateConfiguration);
        Assertions.assertEquals(RetrievalErrorCode.INVALID_REQUEST, exception.getCode());
    }

    @Test
    public void remoteBindingRequiresOptInAndToken() {
        RetrievalProperties properties = new RetrievalProperties();
        Assertions.assertEquals("127.0.0.1", properties.getServerHost());
        properties.setServerHost("0.0.0.0");
        assertInvalid(properties);
        properties.setRemoteAccessEnabled(true);
        assertInvalid(properties);
        properties.setApiToken("test-token");
        properties.validateConfiguration();
        properties.setServerHost("::");
        properties.validateConfiguration();
        properties.setRemoteAccessEnabled(false);
        assertInvalid(properties);
    }
}
