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
        // The score gap is a difference of reciprocal-rank terms: the measured 2Wiki run stays
        // inside [0, 0.003], so a 0.20 default compared against it could never be satisfied.
        Assertions.assertEquals(0.0005D, properties.getHighScoreGap(), 1.0e-12);
        Assertions.assertEquals(12, properties.getLongQueryLength());
        Assertions.assertEquals(0.75D, properties.getStrongAnchorConfidence(), 1.0e-12);
        Assertions.assertEquals(1, properties.getMinimumAnchorCount());
        Assertions.assertEquals(32, properties.getHighCandidateDegree());
        Assertions.assertEquals(2.0D, properties.getHighGrowthRate(), 1.0e-12);
        Assertions.assertEquals(16, properties.getBaseNeighborCap());
    }

    @Test
    public void exposesGraphFusionAndFusionScaleDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        // These were constants inside the routing policy and the fusion config. They set how much of
        // the fused score the graph channel receives, so they must be visible and settable.
        Assertions.assertEquals(2.0D, properties.getGraphFusionGraphWeight(), 1.0e-12);
        Assertions.assertEquals(0.25D, properties.getGraphFusionTextWeight(), 1.0e-12);
        Assertions.assertEquals(0.4D, properties.getGraphProbeShare(), 1.0e-12);
        Assertions.assertEquals(1.25D, properties.getLexicalBm25Weight(), 1.0e-12);
        Assertions.assertEquals(0.85D, properties.getLexicalVectorWeight(), 1.0e-12);
        Assertions.assertEquals(0.85D, properties.getSemanticBm25Weight(), 1.0e-12);
        Assertions.assertEquals(1.25D, properties.getSemanticVectorWeight(), 1.0e-12);
        Assertions.assertEquals(60, properties.getRrfRankConstant());
    }

    @Test
    public void exposesCascadeAndRerankerDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertEquals(0.0001D, properties.getCascadeMinProbeScoreGap(), 1.0e-12);
        Assertions.assertEquals(2, properties.getCascadeMinProbeEvidence());
        Assertions.assertEquals(5, properties.getCascadeMinBudget());
        Assertions.assertEquals(10, properties.getCascadeProbeMax());
        Assertions.assertEquals(2, properties.getCascadeProbeMin());
        Assertions.assertEquals(8, properties.getCascadeProbeDivisor());
        Assertions.assertEquals(0.0D, properties.getRerankerGraphPathWeight(), 1.0e-12);
    }

    @Test
    public void rejectsInvalidGraphFusionAndCascadeSettings() {
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setGraphProbeShare(1.0D)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setGraphProbeShare(-0.1D)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setGraphFusionGraphWeight(-1.0D)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> {
            p.setGraphFusionGraphWeight(0.0D);
            p.setGraphFusionTextWeight(0.0D);
        }));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setRrfRankConstant(0)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setCascadeProbeMin(20)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setCascadeMinProbeEvidence(0)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setCascadeProbeDivisor(0)));
        Assertions.assertThrows(RetrievalException.class, () -> invalid(p -> p.setRerankerGraphPathWeight(-0.5D)));
    }

    @Test
    public void applicationYmlDoesNotOverrideCodeDefaultsWithDifferentValues() throws Exception {
        // The shipped application.yml is a second source of truth: Solon resolves ${key:default} from
        // it first, so a stale yml value silently wins over the @Inject default. That is how
        // high-score-gap stayed at the unreachable 0.20 after the code default became 0.0005.
        java.nio.file.Path yml = java.nio.file.Paths.get("src/main/resources/application.yml");
        if (!java.nio.file.Files.exists(yml)) {
            return;
        }
        java.util.List<String> lines = java.nio.file.Files.readAllLines(yml);
        java.util.Map<String, String> ymlDefaults = new java.util.LinkedHashMap<>();
        String section = "";
        String subsection = "";
        for (String raw : lines) {
            if (raw.trim().isEmpty() || raw.trim().startsWith("#")) {
                continue;
            }
            int indent = raw.indexOf(raw.trim());
            String line = raw.trim();
            if (indent == 0) {
                section = line.endsWith(":") ? line.substring(0, line.length() - 1) : "";
                subsection = "";
            } else if (indent == 2) {
                if (line.endsWith(":")) {
                    subsection = line.substring(0, line.length() - 1);
                } else if (line.contains("${")) {
                    recordYmlDefault(ymlDefaults, section, "", line);
                }
            } else if (indent == 4 && line.contains("${")) {
                recordYmlDefault(ymlDefaults, section, subsection, line);
            }
        }
        Assertions.assertEquals("0.0005", ymlDefaults.get("retrieval.deterministic-rule.high-score-gap"),
            "application.yml must not re-pin the score gap outside the reachable range");
        Assertions.assertEquals("2.0", ymlDefaults.get("retrieval.graph-fusion.graph-weight"));
        Assertions.assertEquals("0.25", ymlDefaults.get("retrieval.graph-fusion.text-weight"));
        Assertions.assertEquals("0.4", ymlDefaults.get("retrieval.graph-fusion.probe-share"));
        Assertions.assertEquals("60", ymlDefaults.get("retrieval.rrf-rank-constant"));
        Assertions.assertEquals("0.0001", ymlDefaults.get("retrieval.cascade.min-probe-score-gap"));
        Assertions.assertEquals("2", ymlDefaults.get("retrieval.cascade.min-probe-evidence"));
        Assertions.assertEquals("0.0", ymlDefaults.get("retrieval.reranker-graph-path-weight"));
    }

    private static void recordYmlDefault(java.util.Map<String, String> target, String section,
                                         String subsection, String line) {
        String key = line.substring(0, line.indexOf(':')).trim();
        String expr = line.substring(line.indexOf("${") + 2, line.lastIndexOf('}'));
        int colon = expr.indexOf(':');
        if (colon < 0) {
            return;
        }
        String path = subsection.isEmpty() ? section + "." + key : section + "." + subsection + "." + key;
        target.put(path, expr.substring(colon + 1));
    }

    private static void invalid(java.util.function.Consumer<RetrievalProperties> mutation) {
        RetrievalProperties properties = new RetrievalProperties();
        mutation.accept(properties);
        properties.validateConfiguration();
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
