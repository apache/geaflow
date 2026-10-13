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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Stable reader lease that pins one immutable graph view until it is closed. */
public final class GraphQueryLease implements GraphNeighborProvider, AutoCloseable {

    private final GraphNeighborProvider provider;
    private final GraphVersion graphVersion;
    private final String deltaDigest;
    private final String statisticsVersion;
    private boolean closed;

    GraphQueryLease(GraphNeighborProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.graphVersion = provider.getGraphVersion();
        this.deltaDigest = provider.getDeltaDigest();
        this.statisticsVersion = provider.getStatisticsVersion();
    }

    private void ensureOpen() {
        if (closed) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY, "graph query lease is closed");
        }
    }

    @Override
    public List<GraphEdgeRef> neighbors(String entityId) {
        ensureOpen();
        return provider.neighbors(entityId);
    }

    @Override
    public GraphVertexRef vertex(EntityRef entity) {
        ensureOpen();
        return provider.vertex(entity);
    }

    @Override
    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    @Override
    public String getDeltaDigest() {
        return deltaDigest;
    }

    @Override
    public String getStatisticsVersion() {
        return statisticsVersion;
    }

    @Override
    public GraphStatistics getStatistics() {
        ensureOpen();
        return provider.getStatistics();
    }

    @Override
    public String getFingerprint() {
        return provider.getFingerprint();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
