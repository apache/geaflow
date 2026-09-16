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

package org.apache.geaflow.ai.graph;

/**
 * The vertex version range that a reported batch of entity changes claims to cover.
 *
 * <p>A derived structure such as a resident keyword index can only apply a batch of changes in
 * place if it can prove the batch is the <em>complete</em> set of vertex level changes since the
 * structure was last known to be correct. Reading the current version after the fact is not a
 * proof: any change made outside the reporting path would be silently accepted as already applied,
 * and the structure would keep serving stale results forever.
 *
 * <p>So the writer states the range instead of the reader guessing it:
 *
 * <pre>
 * VertexVersionWindow window = VertexVersionWindow.open(accessor);
 * ... mutate the graph ...
 * index.onEntitiesUpserted(entities, window.seal());
 * </pre>
 *
 * <p>The consumer accepts the batch only when {@link #getFrom()} matches the version it last
 * accepted <em>and</em> {@link #getTo()} still matches the graph, otherwise it falls back to a
 * full rebuild. Changes that slip in between the writer's own mutations and {@link #seal()} cannot
 * be detected this way; writers that mutate a graph concurrently must serialize themselves.
 */
public final class VertexVersionWindow {

    /** Marks a window that has not been sealed yet, and can therefore not be trusted. */
    private static final long UNSEALED = Long.MIN_VALUE;

    private final GraphAccessor accessor;
    private final long from;
    private final long to;

    private VertexVersionWindow(GraphAccessor accessor, long from, long to) {
        this.accessor = accessor;
        this.from = from;
        this.to = to;
    }

    /**
     * Captures the vertex version before a batch of writes.
     *
     * @param accessor graph the writes will be applied to, may be {@code null}
     */
    public static VertexVersionWindow open(GraphAccessor accessor) {
        long from = accessor == null ? GraphAccessor.VERSION_UNSUPPORTED : accessor.getVertexVersion();
        return new VertexVersionWindow(accessor, from, UNSEALED);
    }

    /**
     * Captures the vertex version after the batch of writes. Call this as close to the last write
     * as possible: everything between the write and this call is a blind spot.
     */
    public VertexVersionWindow seal() {
        long to = accessor == null ? GraphAccessor.VERSION_UNSUPPORTED : accessor.getVertexVersion();
        return new VertexVersionWindow(accessor, from, to);
    }

    public boolean isSealed() {
        return to != UNSEALED;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    /**
     * Whether this window can be trusted to describe every vertex level change between
     * {@code acceptedVersion} and now.
     *
     * @param acceptedVersion version the consumer last accepted as fully applied
     */
    public boolean covers(long acceptedVersion) {
        if (!isSealed() || from != acceptedVersion) {
            return false;
        }
        // Anything that moved the version after the window was sealed is not described by it.
        return accessor == null || accessor.getVertexVersion() == to;
    }

    @Override
    public String toString() {
        return "VertexVersionWindow{from=" + from + ", to=" + (isSealed() ? to : "unsealed") + '}';
    }
}
