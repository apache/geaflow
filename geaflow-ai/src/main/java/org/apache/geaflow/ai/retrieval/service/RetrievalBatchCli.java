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
 */
package org.apache.geaflow.ai.retrieval.service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;

/**
 * Local execution entry point: drives many retrieval requests from a JSONL file.
 *
 * <p>This is a command-line driver, not part of the serving path. It reuses the same recall service
 * as the server so batch and online behaviour cannot diverge, but it is meant for offline runs and
 * benchmark drivers, not for serving traffic.
 */
public final class RetrievalBatchCli {

    private RetrievalBatchCli() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        Path bundle = requiredPath(options, "bundle");
        Path queries = requiredPath(options, "queries");
        final Path output = requiredPath(options, "output");
        String graphName = required(options, "graph-name");
        String graphVersion = required(options, "graph-version");
        String indexVersion = required(options, "index-version");

        RetrievalProperties properties = new RetrievalProperties();
        properties.setArtifactBundleRoot(bundle.toString());
        properties.setArtifactGraphName(graphName);
        properties.setArtifactGraphVersion(graphVersion);
        properties.setArtifactIndexVersion(indexVersion);
        properties.setArtifactVectorSource(required(options, "vector-source"));
        properties.setArtifactVectorVersion(required(options, "vector-version"));
        applyOptionalPaths(options, properties);

        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        ArtifactBundleLoader.loadBundle(bundle, properties, registry);
        RetrievalService service = new RetrievalService(new org.apache.geaflow.ai.service.ServerMemoryCache(),
            properties, new RetrievalMetrics(), registry);

        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        int written = 0;
        try (BufferedReader input = Files.newBufferedReader(queries, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            String line;
            while ((line = input.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                RequestShape shape = parse(line, options);
                String result;
                try {
                    RetrievalResponse response = service.retrieve(shape.request);
                    JsonObject record = new JsonObject();
                    record.addProperty("id", shape.id);
                    record.addProperty("status", 200);
                    record.add("response", new JsonParser().parse(
                        RetrievalApiJson.toJson(response)));
                    result = record.toString();
                } catch (RetrievalException error) {
                    JsonObject record = new JsonObject();
                    record.addProperty("id", shape.id);
                    record.addProperty("status", 400);
                    record.addProperty("code", error.getCode().name());
                    record.addProperty("message", error.getMessage());
                    result = record.toString();
                }
                writer.write(result);
                writer.newLine();
                written++;
            }
        }
        System.out.println("batch retrieval wrote " + written + " results to " + output);
    }

    private static void applyOptionalPaths(Map<String, String> options,
                                           RetrievalProperties properties) {
        if (options.containsKey("graph-path")) {
            properties.setArtifactGraphPath(options.get("graph-path"));
        }
        if (options.containsKey("bm25-path")) {
            properties.setArtifactBm25Path(options.get("bm25-path"));
        }
        if (options.containsKey("vector-path")) {
            properties.setArtifactVectorPath(options.get("vector-path"));
        }
    }

    private static RequestShape parse(String line, Map<String, String> options) {
        JsonObject object = new JsonParser().parse(line).getAsJsonObject();
        final String id = object.has("id") && !object.get("id").isJsonNull()
            ? object.get("id").getAsString()
            : String.valueOf(object.hashCode());
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName(required(options, "graph-name"));
        request.setGraphVersion(required(options, "graph-version"));
        request.setIndexVersion(required(options, "index-version"));
        request.setQuery(text(object, "question", text(object, "query", null)));
        request.setMode(text(object, "mode", options.getOrDefault("mode", "HYBRID")));
        request.setExecutionMode(options.getOrDefault("execution-mode", "SEQUENTIAL"));
        request.setBudget(budget(object, options));
        JsonElement vector = object.get("queryVector");
        if (vector != null && vector.isJsonArray()) {
            List<Double> values = new ArrayList<>();
            for (JsonElement value : vector.getAsJsonArray()) {
                values.add(value.getAsDouble());
            }
            request.setQueryVector(values);
        }
        return new RequestShape(id, request);
    }

    private static RetrievalBudget budget(JsonObject object, Map<String, String> options) {
        JsonObject value = object.has("budget") && object.get("budget").isJsonObject()
            ? object.getAsJsonObject("budget") : new JsonObject();
        int topK = integer(value, "topK", integer(options, "top-k", 10));
        int timeoutMs = integer(value, "timeoutMs", integer(options, "timeout-ms", 10000));
        int maxCandidates = integer(value, "maxCandidates", integer(options, "max-candidates", 100));
        Integer tokenBudget = value.has("tokenBudget") && !value.get("tokenBudget").isJsonNull()
            ? value.get("tokenBudget").getAsInt()
            : integer(options, "token-budget", 4096);
        String constraint = text(value, "primaryConstraint",
            options.getOrDefault("primary-constraint", "CANDIDATES"));
        return new RetrievalBudget(topK, timeoutMs, maxCandidates, tokenBudget,
            BudgetConstraint.parse(constraint));
    }

    private static int integer(JsonObject object, String name, int fallback) {
        return object.has(name) && !object.get(name).isJsonNull()
            ? object.get(name).getAsInt() : fallback;
    }

    private static int integer(Map<String, String> options, String name, int fallback) {
        return options.containsKey(name) ? Integer.parseInt(options.get(name)) : fallback;
    }

    private static String text(JsonObject object, String name, String fallback) {
        return object.has(name) && !object.get(name).isJsonNull()
            ? object.get(name).getAsString() : fallback;
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument: " + argument);
            }
            String name = argument.substring(2);
            int equals = name.indexOf('=');
            if (equals >= 0) {
                values.put(name.substring(0, equals), name.substring(equals + 1));
            } else {
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + argument);
                }
                values.put(name, args[++index]);
            }
        }
        return values;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("--" + name + " is required");
        }
        return value;
    }

    private static Path requiredPath(Map<String, String> options, String name) {
        return Path.of(required(options, name));
    }

    /** One parsed input line: the caller-visible id plus the request to execute. */
    private static final class RequestShape {
        private final String id;
        private final RetrievalRequest request;

        private RequestShape(String id, RetrievalRequest request) {
            this.id = id;
            this.request = request;
        }
    }
}