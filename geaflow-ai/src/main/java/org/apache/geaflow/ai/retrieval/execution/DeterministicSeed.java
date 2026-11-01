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
package org.apache.geaflow.ai.retrieval.execution;

import java.nio.charset.StandardCharsets;

/**
 * Derives deterministic seeds for graph sampling and routing decisions.
 *
 * <p>Seeds are a function of the reproducibility inputs only: normalized query text, the graph
 * version being queried, and the configured base seed. Request-scoped audit identifiers such as
 * {@code requestId} are deliberately excluded, because including them would make neighbor
 * ordering, sampling, and graph paths depend on a value that changes per call. Callers may still
 * propagate a caller-supplied or server-generated {@code requestId}, but only as trace metadata.
 */
public final class DeterministicSeed {

    private static final long FNV_OFFSET_BASIS = 1125899906842597L;
    private static final long FNV_PRIME = 31L;

    private DeterministicSeed() {
    }

    /**
     * Returns the seed used for graph sampling and edge ordering.
     *
     * @param query normalized query text; must not be null
     * @param graphVersion the graph version being queried, or {@code null} when the artifact is
     *     unversioned; a stable placeholder is substituted so that output remains reproducible
     * @param configSeed configured base seed that allows operators to rerun with a different
     *     sampling draw while staying reproducible
     */
    public static long graphSeed(String query, String graphVersion, long configSeed) {
        return mix("graph", query, graphVersion, configSeed);
    }

    /**
     * Returns the seed used by the routing policy context.
     *
     * @param query normalized query text; must not be null
     * @param configSeed configured base seed
     */
    public static long routingSeed(String query, long configSeed) {
        return mix("routing", query, null, configSeed);
    }

    private static long mix(String namespace, String query, String graphVersion, long configSeed) {
        String value = namespace + "\u001f" + (query == null ? "" : query) + "\u001f"
            + (graphVersion == null ? "" : graphVersion) + "\u001f" + configSeed;
        long result = FNV_OFFSET_BASIS;
        for (byte current : value.getBytes(StandardCharsets.UTF_8)) {
            result = FNV_PRIME * result + current;
        }
        return result;
    }
}