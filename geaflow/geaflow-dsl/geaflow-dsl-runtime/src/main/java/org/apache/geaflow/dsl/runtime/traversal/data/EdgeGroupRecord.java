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

package org.apache.geaflow.dsl.runtime.traversal.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath.PathFilterFunction;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath.PathMapFunction;

public class EdgeGroupRecord implements StepRecordWithPath {

    private final EdgeGroup edgeGroup;

    private final Map<Object, ITreePath> targetId2TreePaths;

    private EdgeGroupRecord(EdgeGroup edgeGroup, Map<Object, ITreePath> targetId2TreePaths) {
        this.edgeGroup = Objects.requireNonNull(edgeGroup);
        this.targetId2TreePaths = Objects.requireNonNull(targetId2TreePaths);
    }

    public static EdgeGroupRecord of(EdgeGroup edgeGroup, Map<Object, ITreePath> targetId2TreePaths) {
        return new EdgeGroupRecord(edgeGroup, targetId2TreePaths);
    }

    public EdgeGroup getEdgeGroup() {
        return edgeGroup;
    }

    @Override
    public ITreePath getPathById(Object vertexId) {
        return targetId2TreePaths.get(vertexId);
    }

    @Override
    public Iterable<ITreePath> getPaths() {
        return targetId2TreePaths.values();
    }

    @Override
    public Iterable<Object> getVertexIds() {
        return targetId2TreePaths.keySet();
    }

    @Override
    public StepRecordWithPath filter(PathFilterFunction function, int[] refPathIndices) {
        Map<Object, ITreePath> filterTreePaths = new HashMap<>();
        for (Map.Entry<Object, ITreePath> entry : targetId2TreePaths.entrySet()) {
            Object targetId = entry.getKey();
            ITreePath treePath = entry.getValue();
            ITreePath filterPath = treePath.filter(function, refPathIndices);

            if (!filterPath.isEmpty()) {
                filterTreePaths.put(targetId, filterPath);
            }
        }
        EdgeGroup filterEg;
        if (filterTreePaths.size() == targetId2TreePaths.size()) {
            filterEg = edgeGroup;
        } else {
            filterEg = edgeGroup.filter(edge -> filterTreePaths.containsKey(edge.getTargetId()));
        }
        return new EdgeGroupRecord(filterEg, filterTreePaths);
    }

    @Override
    public <O> List<O> map(PathMapFunction<O> function, int[] refPathIndices) {
        List<O> results = new ArrayList<>();
        for (ITreePath treePath : targetId2TreePaths.values()) {
            List<O> treeResults = treePath.map(function);
            if (treeResults != null) {
                results.addAll(treeResults);
            }
        }
        return results;
    }

    @Override
    public StepRecordWithPath mapPath(PathMapFunction<Path> function, int[] refPathIndices) {
        Map<Object, ITreePath> mapTreePaths = new HashMap<>();
        for (Map.Entry<Object, ITreePath> entry : targetId2TreePaths.entrySet()) {
            Object targetId = entry.getKey();
            ITreePath treePath = entry.getValue();
            ITreePath mapPath = treePath.mapTree(function);
            mapTreePaths.put(targetId, mapPath);
        }
        return new EdgeGroupRecord(edgeGroup, mapTreePaths);
    }

    @Override
    public StepRecordWithPath mapTreePath(Function<ITreePath, ITreePath> function) {
        Map<Object, ITreePath> mapTreePaths = new HashMap<>();
        for (Map.Entry<Object, ITreePath> entry : targetId2TreePaths.entrySet()) {
            Object targetId = entry.getKey();
            ITreePath treePath = entry.getValue();
            ITreePath mapPath = function.apply(treePath);
            mapTreePaths.put(targetId, mapPath);
        }
        return new EdgeGroupRecord(edgeGroup, mapTreePaths);
    }

    @Override
    public StepRecordWithPath subPathSet(int[] pathIndices) {
        if (pathIndices.length == 0) {
            return new EdgeGroupRecord(edgeGroup, new HashMap<>());
        }
        Map<Object, ITreePath> subTreePaths = new HashMap<>();
        for (Map.Entry<Object, ITreePath> entry : targetId2TreePaths.entrySet()) {
            Object targetId = entry.getKey();
            ITreePath treePath = entry.getValue();
            ITreePath subTreePath = treePath.subPath(pathIndices);

            if (!subTreePath.isEmpty()) {
                subTreePaths.put(targetId, subTreePath);
            }
        }
        return new EdgeGroupRecord(edgeGroup, subTreePaths);
    }

    @Override
    public boolean isPathEmpty() {
        return targetId2TreePaths.isEmpty();
    }

    @Override
    public StepRecordType getType() {
        return StepRecordType.EDGE_GROUP;
    }
}
