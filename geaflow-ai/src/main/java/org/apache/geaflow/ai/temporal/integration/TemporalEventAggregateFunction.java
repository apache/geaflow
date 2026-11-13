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

package org.apache.geaflow.ai.temporal.integration;

import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.api.function.base.AggregateFunction;

/**
 * Adapts temporal event integration to GeaFlow keyed aggregation.
 */
public final class TemporalEventAggregateFunction implements
    AggregateFunction<MemoryEvent, IncrementalTemporalIntegrator,
        List<MemoryFactVersion>> {

    @Override
    public IncrementalTemporalIntegrator createAccumulator() {
        return new IncrementalTemporalIntegrator();
    }

    @Override
    public void add(
        MemoryEvent value,
        IncrementalTemporalIntegrator accumulator) {
        Objects.requireNonNull(accumulator, "accumulator")
            .apply(value);
    }

    @Override
    public List<MemoryFactVersion> getResult(
        IncrementalTemporalIntegrator accumulator) {
        return Objects.requireNonNull(
            accumulator,
            "accumulator").snapshot();
    }

    @Override
    public IncrementalTemporalIntegrator merge(
        IncrementalTemporalIntegrator left,
        IncrementalTemporalIntegrator right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");

        IncrementalTemporalIntegrator merged =
            new IncrementalTemporalIntegrator();
        for (MemoryEvent event : left.eventSnapshot()) {
            merged.apply(event);
        }
        for (MemoryEvent event : right.eventSnapshot()) {
            merged.apply(event);
        }
        return merged;
    }
}
