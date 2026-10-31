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

package org.apache.geaflow.ai.retrieval.channel.vector;

import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.index.vector.VectorHit;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;

/** Session-free vector channel using caller supplied query vectors. */
public final class VectorRetriever {
    private final VectorIndexReader reader;

    public VectorRetriever(VectorIndexReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    public List<VectorHit> retrieve(float[] query, int topK, int candidates, long deadlineNanos) {
        return reader.search(query, candidates, topK, deadlineNanos);
    }
}
