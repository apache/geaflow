/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.junit.jupiter.api.Test;

class ArtifactBundleLoaderTest {

    @Test
    void skipsLoadingWhenBundleIsNotConfigured() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();
        ArtifactBundleLoader loader = new ArtifactBundleLoader();
        assertDoesNotThrow(() -> {
            java.lang.reflect.Field field = ArtifactBundleLoader.class.getDeclaredField("properties");
            field.setAccessible(true);
            field.set(loader, properties);
            java.lang.reflect.Field registry = ArtifactBundleLoader.class.getDeclaredField("registry");
            registry.setAccessible(true);
            registry.set(loader, new RetrievalFixtureRegistry());
            loader.initialize();
        });
    }

    @Test
    void rejectsConfiguredMissingBundle() throws Exception {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setArtifactBundleRoot(Files.createTempDirectory("missing-bundle").resolve("release").toString());
        properties.setArtifactGraphName("graph");
        properties.setArtifactGraphVersion("v1");
        properties.setArtifactIndexVersion("v1");
        properties.setArtifactVectorSource("source");
        properties.setArtifactVectorVersion("v1");
        properties.validateConfiguration();
        ArtifactBundleLoader loader = new ArtifactBundleLoader();
        java.lang.reflect.Field field = ArtifactBundleLoader.class.getDeclaredField("properties");
        field.setAccessible(true);
        field.set(loader, properties);
        java.lang.reflect.Field registry = ArtifactBundleLoader.class.getDeclaredField("registry");
        registry.setAccessible(true);
        registry.set(loader, new RetrievalFixtureRegistry());
        assertThrows(RetrievalException.class, loader::initialize);
    }
}
