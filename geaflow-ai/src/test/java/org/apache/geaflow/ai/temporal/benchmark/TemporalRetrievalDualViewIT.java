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

package org.apache.geaflow.ai.temporal.benchmark;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Explicit experiment: run this class in an IDE or with -Dtest=TemporalRetrievalDualViewIT. */
public class TemporalRetrievalDualViewIT {

    @Test
    public void compareFixedTitleBaselineWithWeightedRrf() throws Exception {
        final Path module = moduleRoot();
        final Path configuredOutput = optionalPath("output");
        final Path output = configuredOutput == null
            ? module.getParent().resolve("tmp/retrieval-dual-view-"
                + Instant.now().toString().replace(':', '-')) : configuredOutput;
        final String offline = System.getProperty("temporal.retrieval.dualView.offline", "true");
        Assertions.assertTrue("true".equals(offline) || "false".equals(offline),
            "temporal.retrieval.dualView.offline must be true or false");
        final JsonObject result = new RetrievalDualViewRunner().run(module, optionalPath("input"), output,
            System.getProperty("temporal.retrieval.dualView.device", "cuda"), optionalPath("python"),
            optionalPath("reuseTitleInference"), optionalPath("reuseBodyInference"),
            optionalPath("reuseBodyInput"), Boolean.parseBoolean(offline));
        Assertions.assertEquals("COMPLETED", result.get("status").getAsString());
        Assertions.assertEquals("CE_TITLE_BODY_RRF_75_25", result.get("profile").getAsString());
        Assertions.assertEquals(0.75, result.get("titleWeight").getAsDouble());
        Assertions.assertEquals(0.25, result.get("bodyWeight").getAsDouble());
        Assertions.assertEquals(60, result.get("rrfConstant").getAsInt());
        Assertions.assertEquals(1000, result.get("queryCount").getAsInt());
        Assertions.assertEquals(200, result.get("depth").getAsInt());
        Assertions.assertEquals(20, result.get("finalK").getAsInt());
        Assertions.assertTrue(result.get("pairCount").getAsLong() > 0);
        final JsonArray comparison = result.getAsJsonArray("comparison");
        Assertions.assertEquals(2, comparison.size());
        Assertions.assertEquals("CE_TITLE_BODY", comparison.get(0).getAsJsonObject().get("profile").getAsString());
        Assertions.assertEquals("CE_TITLE_BODY_RRF_75_25",
            comparison.get(1).getAsJsonObject().get("profile").getAsString());
        final Path comparisonFile = output.resolve("comparison.csv");
        Assertions.assertTrue(Files.isRegularFile(comparisonFile));
        System.out.println(new String(Files.readAllBytes(comparisonFile), StandardCharsets.UTF_8));
    }

    private static Path moduleRoot() {
        final Path configured = optionalPath("moduleRoot");
        if (configured != null) {
            return configured.toAbsolutePath().normalize();
        }
        final Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("geaflow-ai/src"))) {
            return current.resolve("geaflow-ai");
        }
        if (Files.isDirectory(current.resolve("src/main/java/org/apache/geaflow/ai"))) {
            return current;
        }
        throw new IllegalArgumentException("Set temporal.retrieval.dualView.moduleRoot to the geaflow-ai directory");
    }

    private static Path optionalPath(String property) {
        final String value = System.getProperty("temporal.retrieval.dualView." + property);
        return value == null || value.trim().isEmpty() ? null : Paths.get(value);
    }
}
