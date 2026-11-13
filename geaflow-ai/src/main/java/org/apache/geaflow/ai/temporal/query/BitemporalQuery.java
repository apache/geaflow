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
import java.util.Objects;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;

/**
 * Selects memory fact versions visible at two temporal points.
 */
public final class BitemporalQuery {

    private static final Comparator<MemoryFactVersion> RESULT_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) ->
                version.getFact().getId())
            .thenComparing(MemoryFactVersion::getId);

    public List<MemoryFactVersion> query(
        List<MemoryFactVersion> versions,
        Instant validAt,
        Instant transactionAt) {
        Objects.requireNonNull(versions, "versions");
        Objects.requireNonNull(validAt, "validAt");
        Objects.requireNonNull(transactionAt, "transactionAt");

        List<MemoryFactVersion> matches = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            Objects.requireNonNull(version, "version");
            if (version.getStatus()
                == MemoryFactVersionStatus.ACTIVE
                && version.getValidTime().contains(validAt)
                && version.getTransactionTime().contains(
                    transactionAt)) {
                matches.add(version);
            }
        }

        Collections.sort(matches, RESULT_ORDER);
        return Collections.unmodifiableList(matches);
    }
}
