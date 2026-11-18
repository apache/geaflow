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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** In-process publication registry with atomic current-version replacement. */
public final class VersionedGraphViewRegistry {

    private final Map<GraphVersion, VersionedGraphView> views = new LinkedHashMap<>();
    private final Map<GraphVersion, Boolean> ready = new LinkedHashMap<>();
    private final Map<String, GraphVersion> published = new LinkedHashMap<>();

    public synchronized void register(GraphSnapshot snapshot) {
        register(snapshot, true);
    }

    public synchronized void register(GraphSnapshot snapshot, boolean isReady) {
        if (snapshot == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "snapshot is required");
        }
        put(snapshot.getGraphVersion(), VersionedGraphView.from(snapshot), isReady);
    }

    public synchronized void register(GraphDelta delta) {
        register(delta, true);
    }

    public synchronized void register(GraphDelta delta, boolean isReady) {
        if (delta == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "delta is required");
        }
        VersionedGraphView parent = views.get(delta.getParentVersion());
        if (parent == null) {
            throw new GraphViewException(GraphViewException.Code.MISSING_PARENT,
                "delta parent is not registered: " + delta.getParentVersion().getVersion());
        }
        if (!ready.getOrDefault(delta.getParentVersion(), false)) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY,
                "delta parent is not READY: " + delta.getParentVersion().getVersion());
        }
        put(delta.getGraphVersion(), VersionedGraphView.from(parent, delta), isReady);
    }

    private void put(GraphVersion version, VersionedGraphView view, boolean isReady) {
        if (views.containsKey(version)) {
            throw new GraphViewException(GraphViewException.Code.VERSION_CONFLICT,
                "graph version is already registered: " + version.getVersion());
        }
        views.put(version, view);
        ready.put(version, isReady);
    }

    /** Atomically publishes a registered READY version, optionally checking its old pointer. */
    public synchronized void publish(GraphVersion version, GraphVersion expectedPublishedVersion) {
        requireVersion(version);
        if (!views.containsKey(version)) {
            throw new GraphViewException(GraphViewException.Code.NOT_FOUND,
                "graph version is not registered: " + version.getVersion());
        }
        if (!ready.getOrDefault(version, false)) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY,
                "graph version is not READY: " + version.getVersion());
        }
        GraphVersion current = published.get(version.getGraphName());
        if (!Objects.equals(current, expectedPublishedVersion)) {
            throw new GraphViewException(GraphViewException.Code.VERSION_CONFLICT,
                "published graph pointer changed");
        }
        published.put(version.getGraphName(), version);
    }

    /** Marks a registered staging version READY after its external artifacts are verified. */
    public synchronized void markReady(GraphVersion version) {
        requireVersion(version);
        if (!views.containsKey(version)) {
            throw new GraphViewException(GraphViewException.Code.NOT_FOUND,
                "graph version is not registered: " + version.getVersion());
        }
        ready.put(version, true);
    }

    public synchronized GraphQueryLease openLease(GraphVersion version) {
        requireVersion(version);
        VersionedGraphView view = views.get(version);
        if (view == null) {
            throw new GraphViewException(GraphViewException.Code.NOT_FOUND,
                "graph version is not registered: " + version.getVersion());
        }
        if (!ready.getOrDefault(version, false)) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY,
                "graph version is not READY: " + version.getVersion());
        }
        return view.openLease();
    }

    public synchronized GraphQueryLease openPublishedLease() {
        if (published.isEmpty()) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY, "no graph version is published");
        }
        if (published.size() != 1) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "graph name is required when multiple graphs are published");
        }
        return openLease(published.values().iterator().next());
    }

    public synchronized Optional<GraphVersion> getPublishedVersion() {
        return published.size() == 1
            ? Optional.of(published.values().iterator().next()) : Optional.empty();
    }

    public synchronized GraphQueryLease openPublishedLease(String graphName) {
        if (graphName == null || graphName.trim().isEmpty()) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "graph name is required");
        }
        GraphVersion version = published.get(graphName);
        if (version == null) {
            throw new GraphViewException(GraphViewException.Code.NOT_READY,
                "no graph version is published for " + graphName);
        }
        return openLease(version);
    }

    public synchronized Optional<GraphVersion> getPublishedVersion(String graphName) {
        return Optional.ofNullable(published.get(graphName));
    }

    public synchronized Optional<VersionedGraphView> find(GraphVersion version) {
        return Optional.ofNullable(views.get(version));
    }

    public synchronized boolean isReady(GraphVersion version) {
        if (version == null) {
            return false;
        }
        return ready.getOrDefault(version, false);
    }

    private static void requireVersion(GraphVersion version) {
        if (version == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "graph version is required");
        }
    }
}
