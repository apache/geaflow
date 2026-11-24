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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;

/** Runs Java preparation and evaluation around the Python-only model scorer. */
public final class RetrievalBaselineRunner {

    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    public JsonObject run(Path moduleRoot, Path inputPath, Path output, String device,
                          Path python, Path reuseInferenceDirectory, boolean offline) throws IOException {
        final Path module = canonical(moduleRoot);
        final Path input = canonical(inputPath == null
            ? module.resolve("target/climate-fever/climate-fever-1.0.1.jsonl") : inputPath);
        final Path worker = module.resolve("bin/score-temporal-cross-encoder.py");
        final Path cache = module.resolve("target/cross-encoder-cache");
        final Path inference = reuseInferenceDirectory == null ? null : canonical(reuseInferenceDirectory);
        final String selectedDevice = device == null ? "cuda" : device.toLowerCase(Locale.ROOT);
        if (!"cuda".equals(selectedDevice) && !"cpu".equals(selectedDevice)) {
            throw new IllegalArgumentException("Device must be cpu or cuda");
        }
        final String environment = "cuda".equals(selectedDevice)
            ? "cross-encoder-cuda-venv" : "cross-encoder-venv";
        final boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");
        final Path executable = canonical(python == null
            ? module.resolve("target/" + environment + (windows ? "/Scripts/python.exe" : "/bin/python")) : python);
        final Path destination = initializeOutput(module, input, output, executable, cache, inference);
        final Path prepared = destination.resolve("prepared");
        final Path scored = inference == null ? destination.resolve("inference") : inference;
        final Path evaluated = destination.resolve("evaluation");
        final JsonArray processes = new JsonArray();
        final String started = Instant.now().toString();
        try {
            Files.createDirectories(destination.resolve("logs"));
            if (!Files.isRegularFile(input)) {
                throw new IOException("The fixed CLIMATE-FEVER dataset is not cached: " + input
                    + ". Supply a local input path; this Java runner does not download datasets.");
            }
            if (inference == null && (!Files.isRegularFile(executable) || !Files.isRegularFile(worker))) {
                throw new IOException("An existing Python model environment and scorer are required: "
                    + executable + "; " + worker);
            }
            if (inference != null && (!Files.isRegularFile(scored.resolve("scores.jsonl"))
                || !Files.isRegularFile(scored.resolve("metadata.json")))) {
                throw new IOException("Reused inference must contain completed scores.jsonl and metadata.json: " + scored);
            }
            System.err.println("Preparing the fixed 1000-query, 200-candidate title/body baseline: " + prepared);
            final ClimateFeverDataset dataset = ClimateFeverDataset.load(input, ClimateFeverDataset.RELEASE_SHA256);
            final JsonObject preparation = new RetrievalBaselinePreparation().prepare(dataset, prepared,
                ClimateFeverDataset.SUBSET_SIZE);
            if (preparation.get("queryCount").getAsInt() != ClimateFeverDataset.SUBSET_SIZE) {
                throw new IOException("The baseline requires all 1000 fixed queries");
            }
            final String preparedHash = sha256(prepared.resolve("manifest.json"));
            if (inference == null) {
                runScoring(scoringCommand(executable, worker, prepared, scored, cache, selectedDevice, offline),
                    module, destination.resolve("logs"), processes);
            } else {
                System.err.println("Reusing completed model scores; Java will verify the regenerated input: " + scored);
            }
            writeJson(destination.resolve("processes.json"), processes, false);
            System.err.println("Evaluating CE_TITLE_BODY at Top20 in Java: " + evaluated);
            final Path scores = scored.resolve("scores.jsonl");
            final Path metadata = scored.resolve("metadata.json");
            final JsonObject evaluation = new RetrievalBaselineEvaluation().evaluate(prepared, scores, metadata, evaluated);
            if (!"retrieval-baseline-results-v1".equals(evaluation.get("schema").getAsString())
                || !preparedHash.equals(evaluation.get("preparedManifestSha256").getAsString())
                || !preparedHash.equals(sha256(prepared.resolve("manifest.json")))
                || !sha256(scores).equals(evaluation.get("scoresSha256").getAsString())
                || !sha256(metadata).equals(evaluation.get("metadataSha256").getAsString())
                || !ClimateFeverDataset.RELEASE_SHA256.equals(sha256(input))) {
                throw new IOException("Dataset, prepared input or model scores changed during the baseline run");
            }
            Files.copy(evaluated.resolve("comparison.csv"), destination.resolve("comparison.csv"));
            final JsonObject model = readObject(metadata);
            final JsonObject manifest = new JsonObject();
            manifest.addProperty("schema", "retrieval-baseline-run-v1");
            manifest.addProperty("status", "COMPLETED");
            manifest.addProperty("profile", "CE_TITLE_BODY");
            manifest.addProperty("depth", 200);
            manifest.addProperty("finalK", 20);
            manifest.addProperty("queryCount", ClimateFeverDataset.SUBSET_SIZE);
            manifest.add("pairCount", preparation.get("pairCount"));
            manifest.addProperty("startedAtUtc", started);
            manifest.addProperty("completedAtUtc", Instant.now().toString());
            manifest.addProperty("inputPath", input.toString());
            manifest.addProperty("rawSha256", ClimateFeverDataset.RELEASE_SHA256);
            manifest.addProperty("releaseCommit", ClimateFeverDataset.RELEASE_COMMIT);
            manifest.addProperty("datasetUrl", ClimateFeverDataset.SOURCE_URL);
            manifest.addProperty("preparedDirectory", prepared.toString());
            manifest.addProperty("evaluationDirectory", evaluated.toString());
            manifest.addProperty("inferenceDirectory", scored.toString());
            manifest.addProperty("inferenceReused", inference != null);
            manifest.addProperty("requestedDevice", selectedDevice);
            manifest.add("device", model.get("device"));
            manifest.addProperty("javaVersion", System.getProperty("java.version"));
            if (inference == null) {
                manifest.addProperty("pythonExecutable", executable.toString());
                manifest.addProperty("scorerSha256", sha256(worker));
            }
            manifest.addProperty("modelDownloadAllowed", !offline && inference == null);
            manifest.addProperty("preparedManifestSha256", preparedHash);
            manifest.addProperty("evaluationManifestSha256", sha256(evaluated.resolve("manifest.json")));
            manifest.addProperty("metadataSha256", evaluation.get("metadataSha256").getAsString());
            manifest.addProperty("scoresSha256", evaluation.get("scoresSha256").getAsString());
            manifest.add("inference", model);
            manifest.add("processResources", processes);
            manifest.add("outputSha256", artifactHashes(destination));
            writeJson(destination.resolve("manifest.json"), manifest, true);
            System.err.println("Retrieval baseline completed: " + destination);
            return manifest;
        } catch (IOException | RuntimeException error) {
            final JsonObject failure = new JsonObject();
            failure.addProperty("schema", "retrieval-baseline-run-v1");
            failure.addProperty("status", "FAILED");
            failure.addProperty("error", error.toString());
            failure.addProperty("preparedDirectory", prepared.toString());
            failure.addProperty("inferenceDirectory", scored.toString());
            failure.addProperty("evaluationDirectory", evaluated.toString());
            failure.add("processResources", processes);
            try {
                writeJson(destination.resolve("processes.json"), processes, false);
                writeJson(destination.resolve("failure.json"), failure, true);
            } catch (IOException recordingError) {
                error.addSuppressed(recordingError);
            }
            throw error;
        }
    }

    static List<String> scoringCommand(Path python, Path worker, Path prepared, Path inference,
                                       Path cache, String device, boolean offline) {
        final List<String> command = new ArrayList<>(Arrays.asList(python.toString(), worker.toString(),
            "--input", prepared.resolve("title-input.jsonl").toString(),
            "--output", inference.resolve("scores.jsonl").toString(),
            "--metadata", inference.resolve("metadata.json").toString(),
            "--cache-dir", cache.toString(), "--device", device,
            "--max-candidates", "200", "--max-length", "512", "--batch-size", "16", "--threads", "4"));
        if (offline) {
            command.add("--local-files-only");
        }
        return command;
    }

    static Path initializeOutput(Path module, Path input, Path output, Path python,
                                 Path cache, Path reusedInference) throws IOException {
        final Path destination = canonical(output);
        if (!Files.isDirectory(module) || module.startsWith(destination)) {
            throw new IllegalArgumentException("Output must be separate from the module root");
        }
        final List<Path> protectedPaths = new ArrayList<>(Arrays.asList(input, python, cache,
            module.resolve("src"), module.resolve("bin"), module.resolve("pom.xml"),
            module.resolve("target/classes"), module.resolve("target/test-classes"),
            module.resolve("target/maven-status"), module.resolve("target/cross-encoder-venv"),
            module.resolve("target/cross-encoder-cuda-venv")));
        if (reusedInference != null) {
            protectedPaths.add(reusedInference);
        }
        for (Path resource : protectedPaths) {
            final Path resolved = canonical(resource);
            if (destination.startsWith(resolved) || resolved.startsWith(destination)) {
                throw new IllegalArgumentException("Output overlaps a baseline input, source, environment or cache: " + resource);
            }
        }
        if (Files.exists(destination)) {
            if (!Files.isDirectory(destination)) {
                throw new IllegalArgumentException("Output must be a new or empty directory");
            }
            try (Stream<Path> entries = Files.list(destination)) {
                if (entries.findAny().isPresent()) {
                    throw new IllegalArgumentException("Output must be a new or empty directory");
                }
            }
        }
        Files.createDirectories(destination);
        return destination;
    }

    static void runScoring(List<String> command, Path module, Path logs,
                           JsonArray processes) throws IOException {
        final JsonObject record = new JsonObject();
        record.addProperty("name", "scoring");
        record.addProperty("executable", command.get(0));
        record.add("arguments", JSON.toJsonTree(command.subList(1, command.size())));
        record.addProperty("status", "RUNNING");
        record.addProperty("stdout", "logs/scoring.stdout.log");
        record.addProperty("stderr", "logs/scoring.stderr.log");
        processes.add(record);
        final long started = System.nanoTime();
        Process process = null;
        try {
            System.err.println("Running Python model scoring; logs: " + logs);
            process = new ProcessBuilder(command).directory(module.toFile())
                .redirectOutput(logs.resolve("scoring.stdout.log").toFile())
                .redirectError(logs.resolve("scoring.stderr.log").toFile()).start();
            final int exit = process.waitFor();
            record.addProperty("exitCode", exit);
            if (exit != 0) {
                throw new IOException("Model scoring failed with exit code " + exit
                    + ". See " + logs.resolve("scoring.stderr.log"));
            }
            record.addProperty("status", "COMPLETED");
        } catch (InterruptedException error) {
            if (process != null) {
                process.destroyForcibly();
            }
            Thread.currentThread().interrupt();
            record.addProperty("status", "FAILED");
            throw new IOException("Model scoring was interrupted; no completed baseline was published", error);
        } catch (IOException | RuntimeException error) {
            record.addProperty("status", "FAILED");
            throw error;
        } finally {
            record.addProperty("wallMillis", (System.nanoTime() - started) / 1_000_000.0);
        }
    }

    static Path canonical(Path path) throws IOException {
        final Path absolute = path.toAbsolutePath().normalize();
        Path existing = absolute;
        while (!Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        return existing.toRealPath().resolve(existing.relativize(absolute)).normalize();
    }

    static JsonObject readObject(Path path) throws IOException {
        return new TemporalJsonCodec().parseObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    static JsonObject artifactHashes(Path root) throws IOException {
        final JsonObject hashes = new JsonObject();
        final List<Path> files;
        try (Stream<Path> paths = Files.walk(root)) {
            files = paths.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
        for (Path path : files) {
            hashes.addProperty(root.relativize(path).toString().replace('\\', '/'), sha256(path));
        }
        return hashes;
    }

    private static String sha256(Path path) throws IOException {
        return RetrievalBaselineEvaluation.sha256(path);
    }

    static void writeJson(Path path, Object value, boolean createNew) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            createNew ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write(JSON.toJson(value));
            writer.write('\n');
        }
    }
}
