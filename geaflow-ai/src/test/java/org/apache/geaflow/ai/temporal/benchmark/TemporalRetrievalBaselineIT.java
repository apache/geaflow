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

import com.google.gson.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Explicit full benchmark: run this class in an IDE or with -Dtest=TemporalRetrievalBaselineIT. */
public class TemporalRetrievalBaselineIT {

    @Test
    public void evaluateFixedTitleBodyBaseline() throws Exception {
        final Path module = moduleRoot();
        final Path configuredOutput = optionalPath("output");
        final Path output = configuredOutput == null
            ? module.getParent().resolve("tmp/retrieval-baseline-"
                + Instant.now().toString().replace(':', '-')) : configuredOutput;
        final String offline = System.getProperty("temporal.retrieval.offline", "true");
        Assertions.assertTrue("true".equals(offline) || "false".equals(offline),
            "temporal.retrieval.offline must be true or false");
        final JsonObject result = new RetrievalBaselineRunner().run(module, optionalPath("input"), output,
            System.getProperty("temporal.retrieval.device", "cuda"), optionalPath("python"),
            optionalPath("reuseInference"), Boolean.parseBoolean(offline));
        Assertions.assertEquals("COMPLETED", result.get("status").getAsString());
        Assertions.assertEquals("CE_TITLE_BODY", result.get("profile").getAsString());
        Assertions.assertEquals(1000, result.get("queryCount").getAsInt());
        Assertions.assertEquals(200, result.get("depth").getAsInt());
        Assertions.assertEquals(20, result.get("finalK").getAsInt());
        Assertions.assertTrue(result.get("pairCount").getAsLong() > 0);
        Assertions.assertTrue(Files.isRegularFile(output.resolve("comparison.csv")));
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
        throw new IllegalArgumentException("Set temporal.retrieval.moduleRoot to the geaflow-ai directory");
    }

    private static Path optionalPath(String property) {
        final String value = System.getProperty("temporal.retrieval." + property);
        return value == null || value.trim().isEmpty() ? null : Paths.get(value);
    }
}
