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

package org.apache.geaflow.dsl.runtime.traversal.path;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.runtime.traversal.message.IPathMessage;

public interface ITreePath extends IPathMessage {

    RowVertex getVertex();

    void setVertex(RowVertex vertex);

    Object getVertexId();

    /**
     * Get node type.
     */
    NodeType getNodeType();

    List<ITreePath> getParents();

    void addParent(ITreePath parent);

    ITreePath merge(ITreePath other);

    EdgeSet getEdgeSet();

    ITreePath copy();

    ITreePath copy(List<ITreePath> parents);

    ITreePath getTreePath(Object requestId);

    Set<Object> getRequestIds();

    void addRequestIds(Collection<Object> requestIds);

    boolean isEmpty();

    int size();

    ITreePath limit(int n);

    ITreePath filter(PathFilterFunction filterFunction, int[] refPathIndices);

    ITreePath mapTree(PathMapFunction<Path> mapFunction);

    <O> List<O> map(PathMapFunction<O> mapFunction);

    <O> List<O> flatMap(PathFlatMapFunction<O> flatMapFunction);

    List<Path> toList();

    ITreePath extendTo(Set<Object> requestIds, List<RowEdge> edges);

    ITreePath extendTo(RowEdge edge);

    ITreePath extendTo(Set<Object> requestIds, RowVertex vertex);

    ITreePath extendTo(RowVertex vertex);

    List<Path> select(int... pathIndices);

    ITreePath subPath(int... pathIndices);

    int getDepth();

    boolean walkTree(List<Object> pathNodes, WalkFunction walkFunction, int maxDepth, PathIdCounter pathId);

    boolean equalNode(ITreePath other);

    ITreePath optimize();

    /**
     * Set request id to all the nodes in the tree.
     */
    void setRequestIdForTree(Object requestId);

    void setRequestId(Object requestId);

    enum NodeType {
        EMPTY_TREE,
        VERTEX_TREE,
        EDGE_TREE,
        UNION_TREE
    }

    interface WalkFunction {

        boolean onWalk(List<Path> paths);
    }

    interface PathFilterFunction {

        boolean accept(Path path);
    }

    interface PathMapFunction<O> {

        O map(Path path);
    }

    interface PathFlatMapFunction<O> {

        Collection<O> flatMap(Path path);
    }

    class PathIdCounter {
        private long counter;

        public PathIdCounter(long counter) {
            this.counter = counter;
        }

        public PathIdCounter() {
            this(0L);
        }

        public long getAndInc() {
            return counter++;
        }
    }
}
