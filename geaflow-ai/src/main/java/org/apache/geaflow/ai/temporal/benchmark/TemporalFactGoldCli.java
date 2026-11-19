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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;

/** Command-line entry point for the fixed four-strategy fact Gold evaluation. */
public final class TemporalFactGoldCli {

    public static void main(String[] arguments) throws IOException {
        final PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
        final PrintStream err = new PrintStream(System.err, true, StandardCharsets.UTF_8.name());
        System.exit(new TemporalFactGoldCli().run(arguments, out, err));
    }

    public int run(String[] arguments, PrintStream out, PrintStream err) {
        try {
            final Map<String, String> options = options(arguments);
            final JsonObject manifest = new FactGoldEvaluation().evaluate(
                Paths.get(options.get("--gold-directory")).toAbsolutePath().normalize(),
                Paths.get(options.get("--output")).toAbsolutePath().normalize());
            out.println(new TemporalJsonCodec().toJson(manifest));
            return manifest.get("passed").getAsBoolean() ? 0 : 1;
        } catch (IllegalArgumentException error) {
            return error(err, 2, "REJECT_INPUT", error.getMessage());
        } catch (IOException | RuntimeException error) {
            return error(err, 3, "REJECT_EXECUTION", error.getMessage());
        }
    }

    private static Map<String, String> options(String[] arguments) {
        final Map<String, String> result = new HashMap<>();
        if (arguments == null || arguments.length != 4) {
            throw new IllegalArgumentException("Required: --gold-directory DIR --output EMPTY_DIR");
        }
        for (int index = 0; index < arguments.length; index += 2) {
            if (!Arrays.asList("--gold-directory", "--output").contains(arguments[index])
                || arguments[index + 1] == null || arguments[index + 1].trim().isEmpty()
                || result.put(arguments[index], arguments[index + 1]) != null) {
                throw new IllegalArgumentException("Required: --gold-directory DIR --output EMPTY_DIR");
            }
        }
        return result;
    }

    private static int error(PrintStream err, int exitCode, String code, String message) {
        final JsonObject json = new JsonObject();
        json.addProperty("code", code);
        json.addProperty("message", message);
        err.println(new TemporalJsonCodec().toJson(json));
        return exitCode;
    }
}
