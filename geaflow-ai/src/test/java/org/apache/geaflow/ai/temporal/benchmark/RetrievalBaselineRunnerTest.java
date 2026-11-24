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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalBaselineRunnerTest {

    @TempDir
    Path directory;

    @Test
    public void testProtectedInputsAndNonemptyOutputRemainUntouched() throws Exception {
        final Path module = Files.createDirectory(directory.resolve("module"));
        final Path cache = Files.createDirectories(module.resolve("target/cross-encoder-cache"));
        final Path weights = cache.resolve("weights.bin");
        final byte[] original = "keep model cache".getBytes(StandardCharsets.UTF_8);
        Files.write(weights, original);
        final Path input = module.resolve("target/climate-fever/raw.jsonl");
        final Path python = module.resolve("target/cross-encoder-cuda-venv/Scripts/python.exe");
        final Path reused = Files.createDirectory(directory.resolve("completed-inference"));
        for (Path output : Arrays.asList(module, module.resolve("src/results"), cache.resolve("new-run"),
            module.resolve("target"), reused.resolve("new-run"))) {
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetrievalBaselineRunner.initializeOutput(module, input, output, python, cache, reused));
        }
        final Path nonempty = Files.createDirectory(directory.resolve("existing-results"));
        final Path marker = nonempty.resolve("manifest.json");
        Files.write(marker, original);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineRunner.initializeOutput(module, input, nonempty, python, cache, reused));
        Assertions.assertArrayEquals(original, Files.readAllBytes(weights));
        Assertions.assertArrayEquals(original, Files.readAllBytes(marker));
        final Path empty = directory.resolve("new-results");
        Assertions.assertEquals(empty.toAbsolutePath(),
            RetrievalBaselineRunner.initializeOutput(module, input, empty, python, cache, reused));
    }

    @Test
    public void testNonzeroProcessStopsAndRetainsErrorLog() throws Exception {
        final boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");
        final Path java = Paths.get(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java");
        final Path logs = Files.createDirectory(directory.resolve("logs with spaces"));
        final JsonArray processes = new JsonArray();
        final IOException failure = Assertions.assertThrows(IOException.class,
            () -> RetrievalBaselineRunner.runScoring(Arrays.asList(java.toString(),
                "-invalid-temporal-test-option"), directory, logs, processes));
        Assertions.assertTrue(failure.getMessage().contains("exit code"));
        Assertions.assertTrue(Files.size(logs.resolve("scoring.stderr.log")) > 0);
        Assertions.assertEquals(1, processes.size());
        final JsonObject record = processes.get(0).getAsJsonObject();
        Assertions.assertEquals("FAILED", record.get("status").getAsString());
        Assertions.assertNotEquals(0, record.get("exitCode").getAsInt());
        Assertions.assertTrue(record.get("wallMillis").getAsDouble() >= 0);
    }
}
