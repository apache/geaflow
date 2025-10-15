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

package org.apache.geaflow.dsl.udf.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntTreePathMessage implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntTreePathMessage.class);

    private IntTreePathMessage[] parents;
    protected Object currentVertexId;

    public IntTreePathMessage() {
        parents = null;
        currentVertexId = null;
    }

    public IntTreePathMessage(IntTreePathMessage[] parents, Object currentVertexId) {
        this.parents = parents;
        this.currentVertexId = currentVertexId;
    }

    public IntTreePathMessage(Object vertexId) {
        parents = null;
        currentVertexId = vertexId;
    }

    public IntTreePathMessage addParentPath(IntTreePathMessage parent) {
        if (parents == null) {
            parents = new IntTreePathMessage[1];
            parents[0] = parent;
        } else {
            IntTreePathMessage[] newParents = new IntTreePathMessage[parents.length + 1];
            System.arraycopy(parents, 0, newParents, 0, parents.length);
            newParents[parents.length] = parent;
            parents = newParents;
        }
        return this;
    }

    public IntTreePathMessage merge(IntTreePathMessage other) {
        if (other == null) {
            return this;
        }
        if (other.parents == null && other.currentVertexId == null) {
            return this;
        }
        if (this.parents == null && this.currentVertexId == null) {
            // 如果当前实例为空，则直接接受其他实例的结构
            this.currentVertexId = other.currentVertexId;
            this.parents = other.parents;
            return this;
        }
        if (!this.currentVertexId.equals(other.currentVertexId)) {
            throw new RuntimeException("merge path failed, all paths should have same tail vertex");
        }
        if (other.parents == null) {
            return this;
        } else if (this.parents == null) {
            this.currentVertexId = other.currentVertexId;
            this.parents = other.parents;
            return this;
        } else {
            for (IntTreePathMessage otherParent : other.parents) {
                boolean merged = false;
                for (IntTreePathMessage parent : this.parents) {
                    if (parent.currentVertexId.equals(otherParent.currentVertexId)) {
                        // 找到同样的父节点，递归合并
                        parent.merge(otherParent);
                        merged = true;
                        break;
                    }
                }
                // 如果未合并，说明需要添加新的父节点
                if (!merged) {
                    IntTreePathMessage[] newParents = Arrays.copyOf(this.parents, this.parents.length + 1);
                    newParents[this.parents.length] = new IntTreePathMessage(otherParent.currentVertexId);
                    newParents[this.parents.length].parents = otherParent.parents;
                    this.parents = newParents;
                }
            }
            return this;
        }
    }

    public IntTreePathMessage addPath(Object[] path) {
        if (path == null || path.length == 0) {
            return this;
        }
        Object head = path[0];
        if (parents == null && currentVertexId == null) {
            currentVertexId = head;
        } else if (!currentVertexId.equals(head)) {
            throw new IllegalArgumentException("Path head does not match current vertex ID. current "
                + currentVertexId + " head " + head);
        }

        if (path.length == 1) {
            return this;
        }

        Object[] subPath = Arrays.copyOfRange(path, 1, path.length);

        if (parents == null) {
            parents = new IntTreePathMessage[]{new IntTreePathMessage(subPath[0])};
            parents[0].addPath(subPath);
        } else {
            boolean pathMerged = false;
            for (IntTreePathMessage parent : parents) {
                if (parent.currentVertexId.equals(subPath[0])) {
                    parent.addPath(subPath);
                    pathMerged = true;
                    break;
                }
            }
            if (!pathMerged) {
                IntTreePathMessage newParent = new IntTreePathMessage(subPath[0]);
                newParent.addPath(subPath);
                parents = Arrays.copyOf(parents, parents.length + 1);
                parents[parents.length - 1] = newParent;
            }
        }
        return this;
    }

    public void extendTo(Integer tailVertexId) {
        if (parents == null && currentVertexId == null) {
            this.currentVertexId = tailVertexId;
            return;
        }
        IntTreePathMessage newMessage = new IntTreePathMessage();
        newMessage.parents = this.parents;
        newMessage.currentVertexId = this.currentVertexId;
        this.parents = new IntTreePathMessage[1];
        this.parents[0] = newMessage;
        this.currentVertexId = tailVertexId;
    }

    public Iterator<Object[]> getPaths() {
        List<Object[]> paths = new ArrayList<>();
        collectPaths(this, new ArrayList<>(), paths);
        return paths.iterator();
    }

    /**
     * get paths, last element is tail vertex id.
     *
     * @param message
     * @param path
     * @param paths
     */
    private void collectPaths(IntTreePathMessage message, List<Object> path, List<Object[]> paths) {
        if (message == null) {
            return;
        }
        if (message.parents == null && message.currentVertexId == null) {
            return;
        }
        path.add(message.currentVertexId);

        if (message.parents == null || message.parents.length == 0) {
            Object[] pathArray = new Object[path.size()];
            for (int i = 0; i < path.size(); i++) {
                pathArray[i] = path.get(path.size() - 1 - i);
            }
            paths.add(pathArray);
        } else {
            for (IntTreePathMessage parent : message.parents) {
                collectPaths(parent, new ArrayList<>(path), paths);
            }
        }
    }

    public long getPathSize() {
        if (parents == null && currentVertexId == null) {
            return 0;
        } else if (parents == null) {
            return 1;
        } else {
            long pathSize = 0L;
            for (int i = 0; i < parents.length; i++) {
                pathSize += parents[i].getPathSize();
            }
            return pathSize;
        }
    }

    public int size() {
        if (parents == null && currentVertexId == null) {
            return 0;
        }
        int cnt = 0;
        Iterator<Object[]> itr = getPaths();
        while (itr.hasNext()) {
            itr.next();
            cnt++;
        }
        return cnt;
    }

    public int getPathLength() {
        return getPathLength(this);
    }

    private int getPathLength(IntTreePathMessage message) {
        if (parents == null && currentVertexId == null) {
            return 0;
        }
        if (parents == null) {
            return 1;
        }
        if (message == null || message.parents == null) {
            return 1;
        }
        int maxLength = 1;
        for (IntTreePathMessage parent : message.parents) {
            maxLength = Math.max(maxLength, 1 + getPathLength(parent));
        }
        return maxLength;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Iterator<Object[]> it = getPaths(); it.hasNext(); ) {
            Object[] path = it.next();
            sb.append(Arrays.toString(path));
        }
        return sb.toString();
    }

    public IntTreePathMessage filter(Integer id) {
        if (this.currentVertexId.equals(id)) {
            return null;
        }
        if (this.parents == null || this.parents.length == 0) {
            return this;
        }
        List<IntTreePathMessage> copyParents = new ArrayList<>();
        for (int i = 0; i < parents.length; i++) {
            IntTreePathMessage copyParent = parents[i].filter(id);
            if (copyParent != null) {
                copyParents.add(copyParent);
            }
        }
        if (copyParents.isEmpty()) {
            return null;
        } else {
            IntTreePathMessage[] copyParentArray = new IntTreePathMessage[copyParents.size()];
            for (int i = 0; i < copyParents.size(); i++) {
                copyParentArray[i] = copyParents.get(i);
            }
            return new IntTreePathMessage(copyParentArray, this.currentVertexId);
        }

    }

    public Object getCurrentVertexId() {
        return currentVertexId;
    }

    public Map<Object, List<Object[]>> generatePathMap() {
        Iterator<Object[]> outPathsIter = this.getPaths();
        Map<Object, List<Object[]>> pathMap = new HashMap<>();
        int preSize = -1;
        while (outPathsIter.hasNext()) {
            Object[] path = outPathsIter.next();
            if (preSize > 0 && path.length != preSize) {
                throw new RuntimeException("meet un equal size " + preSize + " " + path.length);
            }
            preSize = path.length;
            if (!pathMap.containsKey(path[0])) {
                pathMap.put(path[0], new ArrayList<>());
            }
            pathMap.get(path[0]).add(path);
        }
        return pathMap;

    }

    public static class IntTreePathMessageWrapper extends IntTreePathMessage {

        public int tag;

        public IntTreePathMessageWrapper(Object vertexId) {
            super(vertexId);
        }

        public IntTreePathMessageWrapper(Object vertexId, int tag) {
            super(vertexId);
            this.tag = tag;
        }

        public int getTag() {
            return tag;
        }
    }
}