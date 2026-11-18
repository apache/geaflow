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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Exact percentile calculation for small reproducible benchmark samples. */
public final class Percentile {
    private Percentile() {
    }

    public static long calculate(List<Long> values, double quantile) {
        if (values == null || values.isEmpty() || !Double.isFinite(quantile)
            || quantile < 0.0D || quantile > 1.0D) {
            return 0L;
        }
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = (int) Math.ceil(quantile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}
