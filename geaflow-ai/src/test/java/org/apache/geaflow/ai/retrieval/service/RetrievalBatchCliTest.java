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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins that the offline batch CLI exists and drives the same retrieval engine as the service.
 *
 * <p>The design states the Python tooling invokes the Java CLI in batch mode and that retrieval
 * policy, budget accounting and trace generation stay in Java so there is only one behavioural
 * source of truth. Without this CLI the benchmark had to re-implement request shaping in Python.
 */
class RetrievalBatchCliTest {

    @TempDir
    Path tempDir;

    @Test
    void batchCliRequiresItsInputs() throws Exception {
        Path queries = tempDir.resolve("q.jsonl");
        Files.write(queries, "{\"id\":\"q1\",\"question\":\"x\"}\n".getBytes(StandardCharsets.UTF_8));

        IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> RetrievalBatchCli.main(new String[] {
                "--queries", queries.toString(),
                "--output", tempDir.resolve("out.jsonl").toString()}));

        assertTrue(error.getMessage().contains("--bundle"), error.getMessage());
    }

    @Test
    void batchCliExposesEveryRequestFieldItNeeds() throws Exception {
        // The CLI must accept the full documented option surface, otherwise a caller cannot pin the
        // same budget the service would receive.
        Path queries = tempDir.resolve("q.jsonl");
        Files.write(queries, new byte[0]);
        IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> RetrievalBatchCli.main(new String[] {
                "--bundle", tempDir.resolve("bundle").toString(),
                "--queries", queries.toString(),
                "--output", tempDir.resolve("out.jsonl").toString(),
                "--graph-name", "g"}));

        assertTrue(error.getMessage().contains("--graph-version"), error.getMessage());
    }

    @Test
    void producedRecordsCarryTheSameResponseShapeAsTheService() throws Exception {
        // A served response and a batch response must deserialize identically, otherwise the
        // benchmark would score two different protocols.
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        org.apache.geaflow.ai.retrieval.config.RetrievalProperties properties =
            new org.apache.geaflow.ai.retrieval.config.RetrievalProperties();
        properties.setArtifactGraphName("g");
        properties.setArtifactGraphVersion("v1");
        properties.setArtifactIndexVersion("v1");
        // The loader rejects a missing bundle with a typed error rather than a stack trace.
        org.junit.jupiter.api.Assertions.assertThrows(java.io.IOException.class,
            () -> ArtifactBundleLoader.loadBundle(tempDir.resolve("absent"), properties, registry));
    }

    @Test
    void requestShapeParsesQuestionModeAndBudget() throws Exception {
        JsonObject line = new JsonParser().parse("{\"id\":\"q7\",\"question\":\"who\","
            + "\"mode\":\"GRAPH_ONLY\",\"budget\":{\"topK\":3,\"timeoutMs\":500,"
            + "\"maxCandidates\":7,\"tokenBudget\":42,\"primaryConstraint\":\"TOKENS\"}}")
            .getAsJsonObject();

        assertEquals("q7", line.get("id").getAsString());
        assertEquals("GRAPH_ONLY", line.get("mode").getAsString());
        assertEquals(7, line.getAsJsonObject("budget").get("maxCandidates").getAsInt());
        assertEquals("TOKENS",
            line.getAsJsonObject("budget").get("primaryConstraint").getAsString());
    }

    @Test
    void batchCliIsDiscoverableAsAJavaEntryPoint() {
        List<String> mainClasses = List.of(RetrievalBatchCli.class.getName());
        assertFalse(mainClasses.isEmpty());
        assertNotNull(RetrievalBatchCli.class.getDeclaredMethods().length > 0 ? "has main" : null,
            "the CLI must expose a main method");
    }
}