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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalDualViewRunnerTest {

    @TempDir
    Path directory;

    @Test
    public void testBodyCommandKeepsFixedModelSettingsAndUsesBodyInput() {
        final Path input = directory.resolve("body input.jsonl");
        final List<String> command = RetrievalDualViewRunner.bodyScoringCommand(directory.resolve("python"),
            directory.resolve("scorer.py"), input, directory.resolve("inference"), directory.resolve("cache"),
            "cuda", true);
        Assertions.assertEquals(input.toString(), command.get(command.indexOf("--input") + 1));
        Assertions.assertEquals("200", command.get(command.indexOf("--max-candidates") + 1));
        Assertions.assertEquals("512", command.get(command.indexOf("--max-length") + 1));
        Assertions.assertEquals("16", command.get(command.indexOf("--batch-size") + 1));
        Assertions.assertEquals("4", command.get(command.indexOf("--threads") + 1));
        Assertions.assertTrue(command.contains("--local-files-only"));
    }

    @Test
    public void testReusedBodySourcesCannotOverlapOutputOrBeSuppliedAlone() throws Exception {
        final Path module = Files.createDirectory(directory.resolve("module"));
        final Path input = module.resolve("target/climate-fever/raw.jsonl");
        final Path python = module.resolve("target/cross-encoder-cuda-venv/Scripts/python.exe");
        final Path cache = module.resolve("target/cross-encoder-cache");
        final Path body = Files.createDirectories(directory.resolve("historical/body-inference"));
        final Path bodyInput = directory.resolve("historical/prepared/rerank-input.jsonl");
        Files.createDirectories(bodyInput.getParent());
        final byte[] original = "historical evidence".getBytes(StandardCharsets.UTF_8);
        Files.write(bodyInput, original);
        final Path unusedOutput = directory.resolve("unused-output");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewRunner.initializeOutput(module, input, unusedOutput, python, cache,
                null, body, null));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewRunner.initializeOutput(module, input, unusedOutput, python, cache,
                null, null, bodyInput));
        Assertions.assertFalse(Files.exists(unusedOutput));
        for (Path output : Arrays.asList(directory.resolve("historical"), body, body.resolve("nested"),
            bodyInput, bodyInput.resolve("nested"), bodyInput.getParent(),
            bodyInput.getParent().resolve("new-results"))) {
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetrievalDualViewRunner.initializeOutput(module, input, output, python, cache,
                    null, body, bodyInput));
        }
        Assertions.assertArrayEquals(original, Files.readAllBytes(bodyInput));
        try (Stream<Path> files = Files.list(body)) {
            Assertions.assertEquals(0, files.count());
        }
    }

    @Test
    public void testCompletedSourceRunRejectsNewSiblingDirectories() throws Exception {
        final Path module = Files.createDirectory(directory.resolve("module"));
        final Path input = module.resolve("target/climate-fever/raw.jsonl");
        final Path python = module.resolve("target/cross-encoder-cuda-venv/Scripts/python.exe");
        final Path cache = module.resolve("target/cross-encoder-cache");
        final Path historical = Files.createDirectory(directory.resolve("completed-run"));
        final Path inference = Files.createDirectory(historical.resolve("inference"));
        Files.write(historical.resolve("manifest.json"), "{}".getBytes(StandardCharsets.UTF_8));
        final Path attemptedOutput = historical.resolve("new-experiment");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewRunner.initializeOutput(module, input, attemptedOutput, python, cache,
                inference, null, null));
        Assertions.assertFalse(Files.exists(attemptedOutput));
    }
}
