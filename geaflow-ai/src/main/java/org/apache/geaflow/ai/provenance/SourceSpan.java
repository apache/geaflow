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

package org.apache.geaflow.ai.provenance;

import java.util.Objects;

/**
 * Identifies a contiguous span of text within a chunk.
 *
 * <p>A span references positions only. It must never carry raw document text so
 * that provenance records can be logged and persisted without leaking private
 * content.</p>
 */
public class SourceSpan {

    private final String chunkId;
    private final int startOffset;
    private final int endOffset;

    public SourceSpan(String chunkId, int startOffset, int endOffset) {
        this.chunkId = chunkId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    /**
     * Creates a span and eagerly validates it.
     */
    public static SourceSpan of(String chunkId, int startOffset, int endOffset) {
        SourceSpan span = new SourceSpan(chunkId, startOffset, endOffset);
        span.validate();
        return span;
    }

    public String getChunkId() {
        return chunkId;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    /**
     * Validates this span, throwing {@link IllegalArgumentException} when the
     * chunk id is missing or the offsets do not form a valid range.
     */
    public void validate() {
        if (chunkId == null || chunkId.trim().isEmpty()) {
            throw new IllegalArgumentException("source span chunk id is required");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException(
                "source span start offset must be non-negative, got " + startOffset);
        }
        if (endOffset < startOffset) {
            throw new IllegalArgumentException("source span end offset " + endOffset
                + " must be greater than or equal to start offset " + startOffset);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceSpan that = (SourceSpan) o;
        return startOffset == that.startOffset
            && endOffset == that.endOffset
            && Objects.equals(chunkId, that.chunkId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkId, startOffset, endOffset);
    }

    @Override
    public String toString() {
        return "SourceSpan{chunkId='" + chunkId + "', startOffset=" + startOffset
            + ", endOffset=" + endOffset + '}';
    }
}
