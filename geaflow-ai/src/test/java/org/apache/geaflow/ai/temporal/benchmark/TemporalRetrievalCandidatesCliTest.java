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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TemporalRetrievalCandidatesCliTest {

    @TempDir
    Path directory;

    @Test
    public void testRejectsMissingUnknownDuplicateAndEmptyOptions() throws Exception {
        rejected(2);
        rejected(2, "--output", "unused");
        rejected(2, "--module-root", "unused", "--output");
        rejected(2, "--module-root", "unused", "--output", "");
        rejected(2, "--module-root", "unused", "--output", "unused", "--output", "again");
        rejected(2, "--module-root", "unused", "--output", "unused", "--device", "cuda");
        rejected(2, "--module-root", "unused", "--output", "unused", "--expected-sha256", "custom");
    }

    @Test
    public void testMissingPinnedLocalDatasetIsExecutionError() throws Exception {
        final Path output = directory.resolve("missing-input-result");
        rejected(3, "--module-root", directory.toString(), "--output", output.toString());
        Assertions.assertFalse(Files.exists(output.resolve("manifest.json")));
    }

    @Test
    public void testCustomDatasetCannotBypassOfficialHashCheck() throws Exception {
        final Path input = directory.resolve("custom.jsonl");
        final String raw = new Gson().toJson(ClimateFeverDatasetTest.fixture().get(0)) + "\n";
        Files.write(input, raw.getBytes(StandardCharsets.UTF_8));
        final Path output = directory.resolve("custom-result");
        rejected(2, "--module-root", directory.toString(), "--input", input.toString(),
            "--output", output.toString());
        Assertions.assertFalse(Files.exists(output.resolve("manifest.json")));
        Assertions.assertEquals(raw, new String(Files.readAllBytes(input), StandardCharsets.UTF_8));
    }

    @Test
    public void testSourceOutputOverlapIsRejectedBeforeReadingData() throws Exception {
        final Path output = directory.resolve("bin");
        Files.createDirectory(output);
        final Path sentinel = output.resolve("keep.txt");
        Files.write(sentinel, "keep".getBytes(StandardCharsets.UTF_8));
        rejected(2, "--module-root", directory.toString(), "--output", output.toString());
        Assertions.assertEquals("keep", new String(Files.readAllBytes(sentinel), StandardCharsets.UTF_8));
        Assertions.assertFalse(Files.exists(output.resolve("manifest.json")));
    }

    private static void rejected(int exitCode, String... arguments) throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8.name());
             PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8.name())) {
            Assertions.assertEquals(exitCode, new TemporalRetrievalCandidatesCli().run(arguments, out, err));
        }
        Assertions.assertEquals(0, output.size());
        final JsonObject error = new TemporalJsonCodec().parseObject(
            new String(errors.toByteArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals(exitCode == 2 ? "REJECT_INPUT" : "REJECT_EXECUTION",
            error.get("code").getAsString());
        Assertions.assertFalse(error.get("message").getAsString().trim().isEmpty());
    }
}
