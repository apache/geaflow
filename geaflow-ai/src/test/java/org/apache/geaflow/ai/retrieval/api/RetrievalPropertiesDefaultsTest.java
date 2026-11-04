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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the shipped graph-expansion defaults against silently reverting to no-ops.
 *
 * <p>Every control in {@code retrieval.graph} has a value that disables it: a sample rate of
 * {@code 1.0} keeps every neighbour, a marginal-yield floor of {@code 0.0} never trips, and an
 * association floor of {@code 0.0} never filters. A build that ships those values advertises
 * budget-bounded graph expansion while actually performing unbounded expansion, and the unit tests
 * that pass explicit non-default values cannot detect it. These assertions therefore read the
 * out-of-the-box object and the packaged {@code application.yml}, with no setters involved.
 */
public class RetrievalPropertiesDefaultsTest {

    @Test
    public void shippedGraphExpansionDefaultsAreNotNoOps() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        // The sample rate intentionally ships at 1.0: it is a measured recall/cost trade-off, not
        // a value that can be chosen safely for every dataset. What matters is that the control is
        // wired and non-inert when an operator sets it, which the nearby test asserts.
        Assertions.assertEquals(1.0D, properties.getGraphSampleRate(), 1.0e-12,
            "the sample rate ships at full draw; lowering it must be an explicit operator choice");
        Assertions.assertTrue(properties.getGraphMinMarginalYield() > 0.0D,
            "a marginal-yield floor of 0.0 makes MARGINAL_YIELD_LIMIT unreachable");
        Assertions.assertTrue(properties.getGraphMinAssociationScore() > 0.0D,
            "an association floor of 0.0 makes LOW_ASSOCIATION_SCORE unreachable");
        Assertions.assertTrue(properties.getGraphMinAssociationSurvivalRate() > 0.0D,
            "a survival rate of 0.0 makes the association stop rule unreachable");
    }

    /**
     * The sample rate must remain a live control even though its default keeps every neighbour.
     * A value the runtime ignores would make {@code GEAFLOW_RETRIEVAL_GRAPH_SAMPLE_RATE} inert.
     */
    @Test
    public void sampleRateRemainsOperatorConfigurable() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setGraphSampleRate(0.25D);
        properties.validateConfiguration();

        Assertions.assertEquals(0.25D, properties.getGraphSampleRate(), 1.0e-12);

        RetrievalProperties outOfRange = new RetrievalProperties();
        outOfRange.setGraphSampleRate(1.5D);
        assertInvalid(outOfRange);
    }

    @Test
    public void shippedDefaultsDeclareRelationPriorities() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Map<String, Double> priorities = properties.getGraphRelationPriorities();
        Assertions.assertFalse(priorities.isEmpty(),
            "an empty map sends every edge through the 1.0 fallback, so relation-type priority "
                + "never influences neighbour capping");
        Assertions.assertTrue(priorities.containsKey("mentions"),
            "the ingestion pipeline emits 'mentions', so it must be a declared priority");
        Assertions.assertTrue(priorities.containsKey("cooccurs"),
            "the ingestion pipeline emits 'cooccurs', so it must be a declared priority");
    }

    @Test
    public void relationPrioritiesParseInDeclarationOrder() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertIterableEquals(
            java.util.Arrays.asList("mentions", "cooccurs"),
            properties.getGraphRelationPriorities().keySet(),
            "declaration order is the deterministic tie-break among equal weights");
    }

    @Test
    public void malformedRelationPrioritySpecIsRejected() {
        RetrievalProperties missingSeparator = new RetrievalProperties();
        missingSeparator.setGraphRelationPrioritiesSpec("mentions");
        assertInvalid(missingSeparator);

        RetrievalProperties nonNumeric = new RetrievalProperties();
        nonNumeric.setGraphRelationPrioritiesSpec("mentions:high");
        assertInvalid(nonNumeric);

        RetrievalProperties blankLabel = new RetrievalProperties();
        blankLabel.setGraphRelationPrioritiesSpec(":1.0");
        assertInvalid(blankLabel);

        RetrievalProperties negative = new RetrievalProperties();
        negative.setGraphRelationPrioritiesSpec("mentions:-1.0");
        assertInvalid(negative);
    }

    /**
     * The packaged YAML and the field declarations drifted apart before: the file said
     * {@code 0.0} while the {@code @Inject} fallbacks said {@code 0.01}/{@code 0.05}. Only one of
     * them wins at runtime depending on how the process is bootstrapped, so they must agree.
     */
    @Test
    public void packagedYamlMatchesInjectedGraphDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertEquals(Double.parseDouble(yamlGraphValue("sample-rate")),
            properties.getGraphSampleRate(), 1.0e-12);
        Assertions.assertEquals(Double.parseDouble(yamlGraphValue("min-marginal-yield")),
            properties.getGraphMinMarginalYield(), 1.0e-12);
        Assertions.assertEquals(Double.parseDouble(yamlGraphValue("min-association-score")),
            properties.getGraphMinAssociationScore(), 1.0e-12);
        Assertions.assertEquals(Double.parseDouble(yamlGraphValue("min-association-survival-rate")),
            properties.getGraphMinAssociationSurvivalRate(), 1.0e-12);
    }

    @Test
    public void packagedYamlDeclaresRelationPriorities() {
        String spec = yamlGraphValue("relation-priorities");

        Assertions.assertFalse(spec.trim().isEmpty(),
            "application.yml must declare relation-priorities, otherwise the priority control is "
                + "unreachable in a default deployment");
        RetrievalProperties fromYaml = new RetrievalProperties();
        fromYaml.setGraphRelationPrioritiesSpec(spec);
        fromYaml.validateConfiguration();
        Assertions.assertEquals(new LinkedHashMap<>(fromYaml.getGraphRelationPriorities()),
            fromYaml.getGraphRelationPriorities());
        Assertions.assertTrue(fromYaml.getGraphRelationPriorities().containsKey("mentions"));
    }

    private static void assertInvalid(RetrievalProperties properties) {
        Assertions.assertThrows(org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            properties::validateConfiguration);
    }

    /**
     * Reads one packaged {@code retrieval.graph.<key>} default from {@code application.yml},
     * unwrapping a {@code ${ENV:default}} placeholder and ignoring the environment variable so the
     * comparison stays hermetic.
     */
    private static String yamlGraphValue(String key) {
        String yaml = readPackagedYaml();
        String[] lines = yaml.split("\n");
        boolean inGraph = false;
        String prefix = key + ":";
        for (String line : lines) {
            if (line.trim().equals("graph:")) {
                inGraph = true;
                continue;
            }
            if (!inGraph) {
                continue;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            // Leave the graph block as soon as a sibling key at the parent indent appears.
            if (!line.startsWith("    ")) {
                inGraph = false;
                continue;
            }
            if (trimmed.startsWith(prefix)) {
                return unwrapPlaceholder(trimmed.substring(prefix.length()).trim());
            }
        }
        throw new AssertionError("application.yml is missing retrieval.graph." + key);
    }

    private static String unwrapPlaceholder(String value) {
        if (!value.startsWith("${") || !value.endsWith("}")) {
            return value;
        }
        String inner = value.substring(2, value.length() - 1);
        int separator = inner.indexOf(':');
        return separator < 0 ? inner : inner.substring(separator + 1);
    }

    private static String readPackagedYaml() {
        try (InputStream stream = RetrievalPropertiesDefaultsTest.class
            .getResourceAsStream("/application.yml")) {
            Assertions.assertNotNull(stream, "application.yml must be on the test classpath");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new AssertionError("cannot read packaged application.yml", e);
        }
    }
}