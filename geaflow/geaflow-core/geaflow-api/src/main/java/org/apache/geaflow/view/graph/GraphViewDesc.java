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

package org.apache.geaflow.view.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.api.partition.graph.vertex.GraphPartitioner;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.view.IViewDesc;

public class GraphViewDesc implements IViewDesc {

    private final String viewName;
    private final int shardNum;
    private final BackendType backend;
    private final GraphPartitioner partitioner;
    private final GraphMetaType graphMetaType;
    private final Map props;

    private final long currentVersion;

    public GraphViewDesc(String viewName, int shardNum, BackendType backend,
                         GraphPartitioner partitioner, GraphMetaType graphMetaType,
                         Map props, long currentVersion) {
        this.viewName = Objects.requireNonNull(viewName, "view name is null");
        this.shardNum = shardNum;
        this.backend = backend;
        this.partitioner = partitioner;
        this.graphMetaType = graphMetaType;
        this.props = props;
        this.currentVersion = currentVersion;
    }

    @Override
    public String getName() {
        return viewName;
    }

    @Override
    public int getShardNum() {
        return shardNum;
    }

    @Override
    public DataModel getDataModel() {
        return DataModel.GRAPH;
    }

    @Override
    public BackendType getBackend() {
        return backend;
    }

    @Override
    public Map getViewProps() {
        return props == null ? new HashMap() : props;
    }

    public GraphMetaType getGraphMetaType() {
        return graphMetaType;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public GraphSnapshotDesc snapshot(long snapshotVersion) {
        assert !isStatic() : "Only dynamic graph have the snapshot() method.";
        return new GraphSnapshotDesc(viewName, shardNum, backend, partitioner, graphMetaType,
            props, snapshotVersion);
    }

    /**
     * Whether the graph is static or dynamic for graph state format.
     */
    public boolean isStatic() {
        // static graph version is 0
        return currentVersion == 0L;
    }

    public long getCheckpoint(long currentWindowId) {
        if (isStatic()) { // static graph checkpoint is 0
            return 0L;
        } else {
            if (currentVersion <= 0) { // dynamic graph checkpoint start from 1
                return Math.max(currentWindowId, 1L);
            }
            return currentWindowId + currentVersion;
        }
    }

    public GraphViewDesc asStatic() {
        return new GraphViewDesc(viewName, shardNum, backend, partitioner,
            graphMetaType, props, 0L);
    }
}
