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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;

/** Runs the fixed four-profile Evidence evaluation using only local Java retrieval. */
public final class TemporalRetrievalCandidatesCli {

    public static void main(String[] arguments) throws IOException {
        final PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
        final PrintStream err = new PrintStream(System.err, true, StandardCharsets.UTF_8.name());
        System.exit(new TemporalRetrievalCandidatesCli().run(arguments, out, err));
    }

    public int run(String[] arguments, PrintStream out, PrintStream err) {
        try {
            final Map<String, String> options = options(arguments);
            final Path module = RetrievalBaselineRunner.canonical(Paths.get(options.get("--module-root")));
            require(Files.isDirectory(module), "module-root must be an existing directory");
            final Path input = options.containsKey("--input") ? Paths.get(options.get("--input"))
                : module.resolve("target/climate-fever/climate-fever-1.0.1.jsonl");
            final Path output = RetrievalBaselineRunner.canonical(Paths.get(options.get("--output")));
            require(!module.startsWith(output), "Output must be separate from the module root");
            for (String name : new String[] {"src", "bin", "pom.xml", "target/classes", "target/test-classes",
                "target/maven-status", "target/cross-encoder-cache", "target/cross-encoder-venv",
                "target/cross-encoder-cuda-venv"}) {
                final Path resource = RetrievalBaselineRunner.canonical(module.resolve(name));
                require(!output.startsWith(resource) && !resource.startsWith(output),
                    "Output overlaps source, compiled output, an environment or cache: " + name);
            }
            final JsonObject result = new RetrievalCandidateEvaluation().evaluate(input, output);
            out.println(new TemporalJsonCodec().toJson(result));
            return 0;
        } catch (IllegalArgumentException error) {
            return error(err, 2, "REJECT_INPUT", error.getMessage());
        } catch (IOException | RuntimeException error) {
            return error(err, 3, "REJECT_EXECUTION", error.getMessage());
        }
    }

    private static Map<String, String> options(String[] arguments) {
        require(arguments != null && arguments.length > 0 && arguments.length % 2 == 0,
            "Options must be name/value pairs");
        final Map<String, String> options = new HashMap<>();
        for (int index = 0; index < arguments.length; index += 2) {
            require(Arrays.asList("--module-root", "--input", "--output").contains(arguments[index])
                && arguments[index + 1] != null && !arguments[index + 1].trim().isEmpty()
                && options.put(arguments[index], arguments[index + 1]) == null,
                "Unknown, empty or duplicate option");
        }
        require(options.keySet().containsAll(Arrays.asList("--module-root", "--output")),
            "module-root and output are required");
        return options;
    }

    private static int error(PrintStream err, int exitCode, String code, String message) {
        final JsonObject json = new JsonObject();
        json.addProperty("code", code);
        json.addProperty("message", message);
        err.println(new TemporalJsonCodec().toJson(json));
        return exitCode;
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
