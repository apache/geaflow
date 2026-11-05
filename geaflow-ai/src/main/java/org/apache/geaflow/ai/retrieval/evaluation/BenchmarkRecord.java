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
package org.apache.geaflow.ai.retrieval.evaluation;

import java.util.LinkedHashMap;
import java.util.Map;

/** One machine-readable benchmark sample with provenance and measured costs. */
public final class BenchmarkRecord {
    private final Map<String, String> metadata = new LinkedHashMap<>();
    private final Map<String, Double> metrics = new LinkedHashMap<>();

    public BenchmarkRecord metadata(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("metadata key and value are required");
        }
        metadata.put(key, value);
        return this;
    }

    public BenchmarkRecord metric(String key, double value) {
        if (key == null || !Double.isFinite(value)) {
            throw new IllegalArgumentException("metric key and finite value are required");
        }
        metrics.put(key, value);
        return this;
    }

    public Map<String, String> getMetadata() {
        return java.util.Collections.unmodifiableMap(metadata);
    }

    public Map<String, Double> getMetrics() {
        return java.util.Collections.unmodifiableMap(metrics);
    }
}
