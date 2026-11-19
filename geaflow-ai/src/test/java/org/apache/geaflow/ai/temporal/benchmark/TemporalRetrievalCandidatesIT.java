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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Explicit CPU benchmark: run in an IDE or with -Dtest=TemporalRetrievalCandidatesIT. */
public class TemporalRetrievalCandidatesIT {

    @Test
    public void compareFourCandidateProfilesOnFixedEvidence() throws Exception {
        final Path module = moduleRoot();
        final Path configuredOutput = optionalPath("output");
        final Path output = configuredOutput == null
            ? module.getParent().resolve("tmp/retrieval-candidates-"
                + Instant.now().toString().replace(':', '-')) : configuredOutput;
        final List<String> arguments = new ArrayList<>(Arrays.asList(
            "--module-root", module.toString(), "--output", output.toString()));
        final Path input = optionalPath("input");
        if (input != null) {
            arguments.add("--input");
            arguments.add(input.toString());
        }
        final ByteArrayOutputStream standardOutput = new ByteArrayOutputStream();
        final ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(standardOutput, true, StandardCharsets.UTF_8.name());
             PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8.name())) {
            Assertions.assertEquals(0, new TemporalRetrievalCandidatesCli()
                .run(arguments.toArray(new String[0]), out, err),
                new String(errors.toByteArray(), StandardCharsets.UTF_8));
        }
        final JsonObject result = new TemporalJsonCodec().parseObject(
            new String(standardOutput.toByteArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals("retrieval-candidates-v1", result.get("schema").getAsString());
        Assertions.assertEquals("COMPLETED", result.get("status").getAsString());
        Assertions.assertEquals("cpu", result.get("device").getAsString());
        Assertions.assertFalse(result.get("modelInference").getAsBoolean());
        Assertions.assertEquals(1000, result.get("queryCount").getAsInt());
        final JsonArray comparison = result.getAsJsonArray("comparison");
        Assertions.assertEquals(24, comparison.size());
        final Set<String> actual = new HashSet<>();
        final Set<String> expected = new HashSet<>();
        for (String profile : Arrays.asList("RULE_ONLY", "RULE_BM25", "RULE_MINHASH", "RULE_ALL")) {
            for (int k : new int[] {1, 5, 10, 20, 50, 100}) {
                expected.add(profile + ":" + k);
            }
        }
        for (JsonElement element : comparison) {
            final JsonObject row = element.getAsJsonObject();
            Assertions.assertTrue(actual.add(row.get("profile").getAsString() + ":" + row.get("k").getAsInt()));
            Assertions.assertEquals(1000, row.get("evaluatedQueryCount").getAsInt());
        }
        Assertions.assertEquals(expected, actual);
        Assertions.assertTrue(Files.isRegularFile(output.resolve("manifest.json")));
        System.out.println(new String(Files.readAllBytes(output.resolve("comparison.csv")), StandardCharsets.UTF_8));
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
        throw new IllegalArgumentException("Set temporal.retrieval.candidates.moduleRoot to the geaflow-ai directory");
    }

    private static Path optionalPath(String property) {
        final String value = System.getProperty("temporal.retrieval.candidates." + property);
        return value == null || value.trim().isEmpty() ? null : Paths.get(value);
    }
}
