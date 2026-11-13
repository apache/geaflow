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

package org.apache.geaflow.ai.retrieval.ingest;

import org.apache.geaflow.ai.retrieval.metadata.ImportState;
import org.apache.geaflow.ai.retrieval.metadata.MetadataException;

/** Monotonic transitions within one attempt; retries create a new graph version. */
public final class ImportStateMachine {

    private ImportStateMachine() {
    }

    public static boolean canTransition(ImportState current, ImportState target) {
        return current == ImportState.IMPORTING
            && (target == ImportState.INDEXING || target == ImportState.FAILED)
            || current == ImportState.INDEXING
            && (target == ImportState.READY || target == ImportState.FAILED);
    }

    public static void validate(ImportState current, ImportState target) {
        if (!canTransition(current, target)) {
            throw new MetadataException(MetadataException.Code.INVALID_TRANSITION,
                "illegal import transition: " + current + " -> " + target);
        }
    }

    /** Validates and returns the next state for callers driving an import explicitly. */
    public static ImportState transition(ImportState current, ImportState target) {
        validate(current, target);
        return target;
    }

    /** Alias used by adapters that name the operation as a transition validation. */
    public static void validateTransition(ImportState current, ImportState target) {
        validate(current, target);
    }
}
