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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class TreePaths {

    public static ITreePath createTreePath(Iterable<Path> paths) {
        return createTreePath(Lists.newArrayList(paths));
    }

    public static ITreePath singletonPath(Path path) {
        return createTreePath(Collections.singletonList(path));
    }

    public static ITreePath createTreePath(List<Path> paths) {
        return createTreePath(paths, true);
    }

    public static ITreePath createTreePath(List<Path> paths, boolean optimize) {
        if (paths == null) {
            throw new NullPointerException("paths is null");
        }
        if (paths.isEmpty()) {
            return EmptyTreePath.of();
        }
        ITreePath treePath = null;
        for (Path path : paths) {
            ITreePath currentTree = createTreePath(path);
            if (treePath == null) {
                treePath = currentTree;
            } else {
                treePath = treePath.merge(currentTree);
            }
        }
        if (optimize) {
            return treePath.optimize();
        }
        return treePath;
    }

    @SuppressWarnings("unchecked")
    private static ITreePath createTreePath(Path path) {
        ITreePath lastTree = EmptyTreePath.of();
        for (int i = 0; i < path.size(); i++) {
            Row node = path.getField(i, null);
            lastTree = createTreePath(lastTree, node);
        }
        return lastTree;
    }

    private static ITreePath createTreePath(ITreePath lastTree, Row node) {
        ITreePath treePath;
        if (node instanceof RowVertex) {
            RowVertex vertex = (RowVertex) node;
            treePath = lastTree.extendTo(null, vertex);
        } else if (node instanceof RowEdge) {
            RowEdge edge = (RowEdge) node;
            treePath = lastTree.extendTo(null, Lists.newArrayList(edge));
        } else if (node == null) {
            treePath = lastTree.extendTo((RowVertex) null);
        } else {
            throw new GeaflowRuntimeException("TreePath cannot be extended to node: " + node);
        }
        return treePath;
    }
}
