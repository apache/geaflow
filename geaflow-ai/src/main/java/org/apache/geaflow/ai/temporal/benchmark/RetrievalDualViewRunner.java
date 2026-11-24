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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Runs the independent fixed-pool title/body RRF experiment beside its title baseline. */
public final class RetrievalDualViewRunner {

    public JsonObject run(Path moduleRoot, Path inputPath, Path output, String device,
                          Path python, Path reuseTitleInference, Path reuseBodyInference,
                          Path reuseBodyInput, boolean offline) throws IOException {
        final Path module = RetrievalBaselineRunner.canonical(moduleRoot);
        final Path input = RetrievalBaselineRunner.canonical(inputPath == null
            ? module.resolve("target/climate-fever/climate-fever-1.0.1.jsonl") : inputPath);
        final Path titleReuse = canonicalOptional(reuseTitleInference);
        final Path bodyReuse = canonicalOptional(reuseBodyInference);
        final Path bodyReuseInput = canonicalOptional(reuseBodyInput);
        final String selectedDevice = device == null ? "cuda" : device.toLowerCase(Locale.ROOT);
        if (!"cuda".equals(selectedDevice) && !"cpu".equals(selectedDevice)) {
            throw new IllegalArgumentException("Device must be cpu or cuda");
        }
        final String environment = "cuda".equals(selectedDevice)
            ? "cross-encoder-cuda-venv" : "cross-encoder-venv";
        final boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");
        final Path executable = RetrievalBaselineRunner.canonical(python == null
            ? module.resolve("target/" + environment + (windows ? "/Scripts/python.exe" : "/bin/python")) : python);
        final Path worker = module.resolve("bin/score-temporal-cross-encoder.py");
        final Path cache = module.resolve("target/cross-encoder-cache");
        final Path destination = initializeOutput(module, input, output, executable, cache,
            titleReuse, bodyReuse, bodyReuseInput);
        final Path baselineDirectory = destination.resolve("baseline");
        final Path prepared = baselineDirectory.resolve("prepared");
        final Path generatedBodyInput = destination.resolve("body-input.jsonl");
        final Path bodyInput = bodyReuseInput == null ? generatedBodyInput : bodyReuseInput;
        final Path bodyInference = bodyReuse == null ? destination.resolve("body-inference") : bodyReuse;
        final Path titleInference = titleReuse == null ? baselineDirectory.resolve("inference") : titleReuse;
        final Path evaluated = destination.resolve("evaluation");
        final JsonArray processes = new JsonArray();
        final Map<Path, String> sourceHashes = new LinkedHashMap<>();
        final String started = Instant.now().toString();
        try {
            Files.createDirectories(destination.resolve("logs"));
            snapshot(sourceHashes, input);
            if (titleReuse != null) {
                snapshot(sourceHashes, titleReuse.resolve("scores.jsonl"));
                snapshot(sourceHashes, titleReuse.resolve("metadata.json"));
            }
            if (bodyReuse != null) {
                snapshot(sourceHashes, bodyInput);
                snapshot(sourceHashes, bodyInference.resolve("scores.jsonl"));
                snapshot(sourceHashes, bodyInference.resolve("metadata.json"));
            }
            if (titleReuse == null || bodyReuse == null) {
                if (!Files.isRegularFile(executable) || !Files.isRegularFile(worker)) {
                    throw new IOException("An existing Python model environment and scorer are required: "
                        + executable + "; " + worker);
                }
                snapshot(sourceHashes, worker);
            }
            final JsonObject baseline = new RetrievalBaselineRunner().run(module, input, baselineDirectory,
                selectedDevice, executable, titleReuse, offline);
            final String preparedHash = sha256(prepared.resolve("manifest.json"));
            final String baselineHash = sha256(baselineDirectory.resolve("manifest.json"));
            System.err.println("Preparing the same 200-candidate body-only scoring input: " + generatedBodyInput);
            RetrievalBodyViewScores.prepare(prepared, generatedBodyInput);
            final String generatedBodyHash = sha256(generatedBodyInput);
            sourceHashes.put(generatedBodyInput, generatedBodyHash);
            sourceHashes.put(prepared.resolve("manifest.json"), preparedHash);
            sourceHashes.put(baselineDirectory.resolve("manifest.json"), baselineHash);
            if (bodyReuse == null) {
                try {
                    RetrievalBaselineRunner.runScoring(bodyScoringCommand(executable, worker,
                        generatedBodyInput, bodyInference, cache, selectedDevice, offline),
                        module, destination.resolve("logs"), processes);
                } finally {
                    if (processes.size() > 0) {
                        processes.get(0).getAsJsonObject().addProperty("name", "body-scoring");
                    }
                }
            } else {
                System.err.println("Reusing body scores with their original input; Java verifies and projects by ID: "
                    + bodyInference);
            }
            RetrievalBaselineRunner.writeJson(destination.resolve("processes.json"), processes, false);
            System.err.println("Comparing CE_TITLE_BODY and CE_TITLE_BODY_RRF_75_25 at Top20: " + evaluated);
            final Path titleScores = titleInference.resolve("scores.jsonl");
            final Path titleMetadata = titleInference.resolve("metadata.json");
            final JsonObject evaluation = new RetrievalDualViewEvaluation().evaluate(prepared,
                titleScores, titleMetadata, bodyInput, bodyInference.resolve("scores.jsonl"),
                bodyInference.resolve("metadata.json"), evaluated);
            if (!"retrieval-dual-view-results-v1".equals(evaluation.get("schema").getAsString())
                || evaluation.get("queryCount").getAsInt() != ClimateFeverDataset.SUBSET_SIZE
                || !preparedHash.equals(evaluation.get("preparedManifestSha256").getAsString())
                || !preparedHash.equals(sha256(prepared.resolve("manifest.json")))
                || !baselineHash.equals(sha256(baselineDirectory.resolve("manifest.json")))
                || !generatedBodyHash.equals(sha256(generatedBodyInput))
                || !sha256(titleScores).equals(evaluation.get("titleScoresSha256").getAsString())
                || !sha256(titleMetadata).equals(evaluation.get("titleMetadataSha256").getAsString())
                || !baseline.get("scoresSha256").equals(evaluation.get("titleScoresSha256"))
                || !baseline.get("metadataSha256").equals(evaluation.get("titleMetadataSha256"))
                || !ClimateFeverDataset.RELEASE_SHA256.equals(sha256(input))) {
                throw new IOException("Dataset, prepared input, baseline or model artifacts changed during the experiment");
            }
            final JsonObject bodySource = evaluation.getAsJsonObject("bodySource");
            sourceHashes.putIfAbsent(titleScores, evaluation.get("titleScoresSha256").getAsString());
            sourceHashes.putIfAbsent(titleMetadata, evaluation.get("titleMetadataSha256").getAsString());
            sourceHashes.putIfAbsent(bodyInput, bodySource.get("inputSha256").getAsString());
            sourceHashes.putIfAbsent(bodyInference.resolve("scores.jsonl"),
                bodySource.get("scoresSha256").getAsString());
            sourceHashes.putIfAbsent(bodyInference.resolve("metadata.json"),
                bodySource.get("metadataSha256").getAsString());
            Files.copy(evaluated.resolve("comparison.csv"), destination.resolve("comparison.csv"));
            final JsonObject manifest = new JsonObject();
            manifest.addProperty("schema", "retrieval-dual-view-run-v1");
            manifest.addProperty("status", "COMPLETED");
            manifest.addProperty("profile", "CE_TITLE_BODY_RRF_75_25");
            manifest.addProperty("titleWeight", 0.75);
            manifest.addProperty("bodyWeight", 0.25);
            manifest.addProperty("rrfConstant", 60);
            manifest.addProperty("depth", 200);
            manifest.addProperty("finalK", 20);
            manifest.addProperty("queryCount", ClimateFeverDataset.SUBSET_SIZE);
            manifest.add("pairCount", evaluation.get("pairCount"));
            manifest.addProperty("startedAtUtc", started);
            manifest.addProperty("completedAtUtc", Instant.now().toString());
            manifest.addProperty("inputPath", input.toString());
            manifest.addProperty("rawSha256", ClimateFeverDataset.RELEASE_SHA256);
            manifest.addProperty("releaseCommit", ClimateFeverDataset.RELEASE_COMMIT);
            manifest.addProperty("datasetUrl", ClimateFeverDataset.SOURCE_URL);
            manifest.addProperty("baselineDirectory", baselineDirectory.toString());
            manifest.addProperty("baselineManifestSha256", baselineHash);
            manifest.addProperty("preparedDirectory", prepared.toString());
            manifest.addProperty("preparedManifestSha256", preparedHash);
            manifest.addProperty("generatedBodyInputPath", generatedBodyInput.toString());
            manifest.addProperty("generatedBodyInputSha256", generatedBodyHash);
            manifest.addProperty("evaluationDirectory", evaluated.toString());
            manifest.addProperty("evaluationManifestSha256", sha256(evaluated.resolve("manifest.json")));
            manifest.addProperty("titleInferenceReused", titleReuse != null);
            manifest.addProperty("bodyInferenceReused", bodyReuse != null);
            manifest.addProperty("titleInferenceDirectory", titleInference.toString());
            manifest.addProperty("bodyInferenceDirectory", bodyInference.toString());
            manifest.addProperty("requestedDevice", selectedDevice);
            manifest.addProperty("javaVersion", System.getProperty("java.version"));
            manifest.addProperty("modelDownloadAllowed", !offline && (titleReuse == null || bodyReuse == null));
            if (titleReuse == null || bodyReuse == null) {
                manifest.addProperty("pythonExecutable", executable.toString());
                manifest.addProperty("scorerSha256", sourceHashes.get(worker));
            }
            manifest.add("titleScoresPath", evaluation.get("titleScoresPath"));
            manifest.add("titleMetadataPath", evaluation.get("titleMetadataPath"));
            manifest.add("titleScoresSha256", evaluation.get("titleScoresSha256"));
            manifest.add("titleMetadataSha256", evaluation.get("titleMetadataSha256"));
            manifest.add("titleInference", evaluation.get("titleInference"));
            manifest.add("bodySource", evaluation.get("bodySource"));
            manifest.add("comparison", evaluation.get("comparison"));
            manifest.add("baselineProcessResources", baseline.get("processResources"));
            manifest.add("processResources", processes);
            manifest.add("outputSha256", RetrievalBaselineRunner.artifactHashes(destination));
            verifySources(sourceHashes);
            RetrievalBaselineRunner.writeJson(destination.resolve("manifest.json"), manifest, true);
            System.err.println("Retrieval dual-view experiment completed: " + destination);
            return manifest;
        } catch (IOException | RuntimeException error) {
            final JsonObject failure = new JsonObject();
            failure.addProperty("schema", "retrieval-dual-view-run-v1");
            failure.addProperty("status", "FAILED");
            failure.addProperty("error", error.toString());
            failure.addProperty("baselineDirectory", baselineDirectory.toString());
            failure.addProperty("preparedDirectory", prepared.toString());
            failure.addProperty("bodyInputPath", bodyInput.toString());
            failure.addProperty("titleInferenceDirectory", titleInference.toString());
            failure.addProperty("bodyInferenceDirectory", bodyInference.toString());
            failure.addProperty("evaluationDirectory", evaluated.toString());
            failure.add("processResources", processes);
            try {
                RetrievalBaselineRunner.writeJson(destination.resolve("processes.json"), processes, false);
                RetrievalBaselineRunner.writeJson(destination.resolve("failure.json"), failure, true);
            } catch (IOException recordingError) {
                error.addSuppressed(recordingError);
            }
            throw error;
        }
    }

    static List<String> bodyScoringCommand(Path python, Path worker, Path input, Path inference,
                                           Path cache, String device, boolean offline) {
        final List<String> command = RetrievalBaselineRunner.scoringCommand(python, worker,
            input.getParent(), inference, cache, device, offline);
        command.set(command.indexOf("--input") + 1, input.toString());
        return command;
    }

    static Path initializeOutput(Path module, Path input, Path output, Path python, Path cache,
                                 Path reusedTitle, Path reusedBody, Path reusedBodyInput) throws IOException {
        if ((reusedBody == null) != (reusedBodyInput == null)) {
            throw new IllegalArgumentException("reuseBodyInference and reuseBodyInput must be supplied together");
        }
        if (output == null) {
            throw new IllegalArgumentException("An explicit new or empty experiment output directory is required");
        }
        final Path destination = RetrievalBaselineRunner.canonical(output);
        for (Path resource : Arrays.asList(reusedBody, reusedBodyInput == null ? null : reusedBodyInput.getParent(),
            completedRun(reusedTitle), completedRun(reusedBody), completedRun(reusedBodyInput))) {
            if (resource != null) {
                final Path resolved = RetrievalBaselineRunner.canonical(resource);
                if (destination.startsWith(resolved) || resolved.startsWith(destination)) {
                    throw new IllegalArgumentException("Output overlaps a reused experiment source: " + resource);
                }
            }
        }
        return RetrievalBaselineRunner.initializeOutput(module, input, destination, python, cache, reusedTitle);
    }

    private static Path completedRun(Path resource) {
        if (resource == null || resource.getParent() == null) {
            return null;
        }
        final Path parent = resource.getParent();
        if (Files.isRegularFile(parent.resolve("manifest.json"))) {
            return parent;
        }
        final Path grandparent = parent.getParent();
        return grandparent != null && Files.isRegularFile(grandparent.resolve("manifest.json"))
            ? grandparent : null;
    }

    private static Path canonicalOptional(Path path) throws IOException {
        return path == null ? null : RetrievalBaselineRunner.canonical(path);
    }

    private static void snapshot(Map<Path, String> hashes, Path path) throws IOException {
        hashes.put(path, sha256(path));
    }

    private static void verifySources(Map<Path, String> hashes) throws IOException {
        for (Map.Entry<Path, String> entry : hashes.entrySet()) {
            if (!entry.getValue().equals(sha256(entry.getKey()))) {
                throw new IOException("Experiment source changed during the run: " + entry.getKey());
            }
        }
    }

    private static String sha256(Path path) throws IOException {
        return RetrievalBaselineEvaluation.sha256(path);
    }
}
