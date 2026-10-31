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

package org.apache.geaflow.ai.temporal.query;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/**
 * Queries a committed temporal state without choosing a winner among conflicts.
 * Results use FactKey, valid interval, system interval and version ID order;
 * an open interval end sorts after every finite end.
 */
public final class TemporalQueryService {

    private static final Comparator<Instant> END_ORDER =
        Comparator.nullsLast(Comparator.naturalOrder());

    private static final Comparator<MemoryFactVersion> VERSION_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) -> version.getValidTime().getStart())
            .thenComparing(
                version -> version.getValidTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(version -> version.getTransactionTime().getStart())
            .thenComparing(
                version -> version.getTransactionTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(MemoryFactVersion::getId);

    /** Returns active versions with an open system end at the given valid time. */
    public List<MemoryFactVersion> current(
        TemporalState state, FactKey factKey, Instant validAt) {
        Objects.requireNonNull(factKey, "factKey");
        return select(state, factKey, validAt, null, Mode.CURRENT);
    }

    public List<MemoryFactVersion> current(TemporalState state, Instant validAt) {
        return select(state, null, validAt, null, Mode.CURRENT);
    }

    /** Returns all audit versions, including closed versions and tombstones. */
    public List<MemoryFactVersion> historical(
        TemporalState state, FactKey factKey, Instant validAt) {
        Objects.requireNonNull(factKey, "factKey");
        return select(state, factKey, validAt, null, Mode.HISTORICAL);
    }

    public List<MemoryFactVersion> historical(TemporalState state, Instant validAt) {
        return select(state, null, validAt, null, Mode.HISTORICAL);
    }

    /** Returns active versions visible at both half-open temporal coordinates. */
    public List<MemoryFactVersion> asOf(
        TemporalState state, FactKey factKey, Instant validAt, Instant systemAt) {
        Objects.requireNonNull(factKey, "factKey");
        return select(state, factKey, validAt, systemAt, Mode.AS_OF);
    }

    public List<MemoryFactVersion> asOf(
        TemporalState state, Instant validAt, Instant systemAt) {
        return select(state, null, validAt, systemAt, Mode.AS_OF);
    }

    private static List<MemoryFactVersion> select(
        TemporalState state,
        FactKey factKey,
        Instant validAt,
        Instant systemAt,
        Mode mode) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(validAt, "validAt");
        if (mode == Mode.AS_OF) {
            Objects.requireNonNull(systemAt, "systemAt");
        }

        List<MemoryFactVersion> result = new ArrayList<>();
        // TemporalState already orders its groups by the full FactKey, including scope.
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            if (factKey != null && !factKey.equals(entry.getKey())) {
                continue;
            }
            List<MemoryFactVersion> matches = new ArrayList<>();
            for (MemoryFactVersion version : entry.getValue()) {
                if (!version.getValidTime().contains(validAt)) {
                    continue;
                }
                if (mode != Mode.HISTORICAL) {
                    if (version.getStatus() != MemoryFactVersionStatus.ACTIVE) {
                        continue;
                    }
                    if (mode == Mode.CURRENT
                        ? version.getTransactionTime().getEnd().isPresent()
                        : !version.getTransactionTime().contains(systemAt)) {
                        continue;
                    }
                }
                matches.add(version);
            }
            Collections.sort(matches, VERSION_ORDER);
            result.addAll(matches);
        }
        return Collections.unmodifiableList(result);
    }

    private enum Mode {
        CURRENT,
        HISTORICAL,
        AS_OF
    }
}
