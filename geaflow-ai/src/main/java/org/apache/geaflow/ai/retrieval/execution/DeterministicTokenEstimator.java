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

package org.apache.geaflow.ai.retrieval.execution;

import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;

/** Reproducible estimator that honors artifact estimates and falls back to four characters per token. */
public final class DeterministicTokenEstimator implements TokenEstimator {
    public static final String VERSION = "chars4-v1";
    /** Characters per token assumed by this estimator; the version string encodes the same value. */
    public static final int CHARACTERS_PER_TOKEN = 4;
    private static final int ROUND_UP_OFFSET = CHARACTERS_PER_TOKEN - 1;

    @Override
    public int estimate(TextChunk chunk) {
        Objects.requireNonNull(chunk, "chunk");
        if (chunk.getTokenEstimate() > 0) {
            return chunk.getTokenEstimate();
        }
        long characters = (long) chunk.getText().length();
        return (int) Math.max(1L, (characters + ROUND_UP_OFFSET) / CHARACTERS_PER_TOKEN);
    }

    @Override
    public String getVersion() {
        return VERSION;
    }
}
