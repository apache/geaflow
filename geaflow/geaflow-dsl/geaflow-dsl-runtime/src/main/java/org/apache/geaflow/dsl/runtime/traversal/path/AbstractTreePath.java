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

import static org.apache.geaflow.dsl.runtime.traversal.path.TreePaths.createTreePath;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignPath;
import org.apache.geaflow.dsl.runtime.traversal.message.IMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;

public abstract class AbstractTreePath implements ITreePath {

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void addParent(ITreePath parent) {
        if (parent.getNodeType() != NodeType.EMPTY_TREE) {
            getParents().add(parent);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ITreePath merge(ITreePath other) {
        if (other.getNodeType() == NodeType.UNION_TREE) {
            return other.merge(this);
        } else if (other.getNodeType() == NodeType.EMPTY_TREE) {
            return this;
        }
        if (other.getNodeType() != getNodeType()
            && !(getNodeType() == NodeType.VERTEX_TREE && getVertex() == null)
            && !(other.getNodeType() == NodeType.VERTEX_TREE && other.getVertex() == null)) {
            throw new GeaFlowDSLException("Merge with different tree kinds: " + getNodeType()
                + " and " + other.getNodeType());
        }

        if (this.equalNode(other) && getDepth() == other.getDepth()) {
            for (ITreePath parent : other.getParents()) {
                addParent(parent);
            }
            this.addRequestIds(other.getRequestIds());
            return this;
        } else {
            return UnionTreePath.create(Lists.newArrayList(this, other)).optimize();
        }
    }

    @Override
    public ITreePath optimize() {
        if (getParents().isEmpty()) {
            return this;
        }
        List<ITreePath> optimizedParents = new ArrayList<>(getParents().size());
        for (ITreePath parent : getParents()) {
            optimizedParents.add(parent.optimize());
        }

        List<ITreePath> mergedParents = new ArrayList<>(optimizedParents.size());
        mergedParents.add(optimizedParents.get(0).copy());

        for (int i = 1; i < optimizedParents.size(); i++) {
            ITreePath parent = optimizedParents.get(i);
            mergeParent(parent, mergedParents);
        }
        return copy(mergedParents);
    }

    private void mergeParent(ITreePath parent, List<ITreePath> mergedParents) {
        boolean hasMerged = false;
        if (parent.getNodeType() == NodeType.EDGE_TREE) {
            for (ITreePath mergedParent : mergedParents) {
                if (mergedParent.getNodeType() == NodeType.EDGE_TREE
                    && mergedParent.getEdgeSet().like(parent.getEdgeSet())
                    && Objects.equals(mergedParent.getParents(), parent.getParents())) {
                    mergedParent.getEdgeSet().addEdges(parent.getEdgeSet());
                    hasMerged = true;
                    break;
                }
            }
        }
        if (!hasMerged) {
            mergedParents.add(parent);
        }
    }


    @Override
    public ITreePath extendTo(Set<Object> requestIds, List<RowEdge> edges) {
        EdgeSet edgeSet = new DefaultEdgeSet(edges);
        ITreePath newTreePath = EdgeTreePath.of(requestIds, edgeSet);
        newTreePath.addParent(this);
        return newTreePath;
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, RowVertex vertex) {
        ITreePath newTreePath = VertexTreePath.of(requestIds, vertex);
        newTreePath.addParent(this);
        return newTreePath;
    }

    @Override
    public ITreePath limit(int n) {
        if (n < 0 || n == Integer.MAX_VALUE) {
            return this;
        }
        final List<Path> limitPaths = new ArrayList<>();
        walkTree(paths -> {
            int rest = n - limitPaths.size();
            if (rest > 0) {
                limitPaths.addAll(paths.subList(0, Math.min(rest, paths.size())));
                return true;
            } else {
                return false;
            }
        });
        return createTreePath(limitPaths);
    }

    @Override
    public ITreePath filter(PathFilterFunction filterFunction, int[] refPathIndices) {
        int depth = getDepth();
        int[] mapping = createMappingIndices(refPathIndices, depth);
        return filter(filterFunction, refPathIndices, mapping, new DefaultPath(), depth, new PathIdCounter());
    }

    /**
     * Filter on the tree path.This is a fast implementation which filter the tree without
     * expand this tree and rebuild the filter paths to a tree.
     */
    protected ITreePath filter(PathFilterFunction filterFunction,
                               int[] refPathIndices,
                               int[] fieldMapping,
                               Path currentPath,
                               int maxDepth,
                               PathIdCounter pathId) {
        if (refPathIndices.length == 0) {
            // filter function has not referred any fields in the path.
            if (filterFunction.accept(null)) {
                return this;
            }
            return EmptyTreePath.of();
        }
        int pathIndex = maxDepth - currentPath.size() - 1;
        int parentSize = getParents().size();
        switch (getNodeType()) {
            case VERTEX_TREE:
                currentPath.addNode(getVertex());
                break;
            case EDGE_TREE:
                // If this edge set is referred by the filter function, do filter
                // for each edge in the set.
                if (Arrays.binarySearch(refPathIndices, pathIndex) >= 0) {
                    EdgeSet edges = getEdgeSet();
                    List<ITreePath> filterTrees = new ArrayList<>();
                    for (RowEdge edge : edges) {
                        currentPath.addNode(edge);
                        // if the parent is empty or reach the last referred path node in the filter function.
                        if (parentSize == 0 || pathIndex == refPathIndices[0]) {
                            // Align the field indices of the current path with the referred index in the function.
                            FieldAlignPath alignPath = new FieldAlignPath(currentPath, fieldMapping);
                            alignPath.setId(pathId.getAndInc());
                            if (filterFunction.accept(alignPath)) {
                                EdgeTreePath edgeTreePath = EdgeTreePath.of(null, edge);
                                if (parentSize > 0) {
                                    edgeTreePath.addParent(getParents().get(0));
                                }
                                filterTrees.add(edgeTreePath);
                            }
                        } else if (parentSize >= 1) {
                            for (ITreePath parent : getParents()) {
                                ITreePath filterTree = ((AbstractTreePath) parent).filter(filterFunction,
                                    refPathIndices, fieldMapping, currentPath, maxDepth, pathId);
                                if (!filterTree.isEmpty()) {
                                    filterTrees.add(filterTree.extendTo(edge));
                                }
                            }
                        }
                        currentPath.remove(currentPath.size() - 1);
                    }
                    return UnionTreePath.create(filterTrees);
                } else { // edge is not referred in the filter function, so add null to the current path.
                    currentPath.addNode(null);
                }
                break;
            default:
                throw new IllegalArgumentException("Illegal tree node: " + getNodeType());
        }
        // reach the last referred path node. (refPathIndices is sorted, so refPathIndices[0] is the
        // last referred path field).
        if (pathIndex == refPathIndices[0]) {
            // Align the field indices of the current path with the referred index in the function.
            FieldAlignPath alignPath = new FieldAlignPath(currentPath, fieldMapping);
            alignPath.setId(pathId.getAndInc());
            boolean accept = filterFunction.accept(alignPath);
            // remove current node before return.
            currentPath.remove(currentPath.size() - 1);
            if (accept) {
                return this;
            }
            return EmptyTreePath.of();
        }
        // filter parent tree
        List<ITreePath> filterParents = new ArrayList<>(parentSize);
        for (ITreePath parent : getParents()) {
            ITreePath filterTree = ((AbstractTreePath) parent).filter(filterFunction, refPathIndices,
                fieldMapping, currentPath, maxDepth, pathId);
            if (!filterTree.isEmpty()) {
                filterParents.add(filterTree);
            }
        }
        // remove current node before return.
        currentPath.remove(currentPath.size() - 1);
        if (filterParents.size() > 0) {
            return copy(filterParents);
        }
        // If all the parents has be filtered, then this tree will be filtered and just return an empty tree
        // to the child node.
        return EmptyTreePath.of();
    }

    /**
     * Create field mapping for the referred path indices.
     *
     * <p>e.g. The refPathIndices is: [1, 3], the total path field is: 4, then the path layout is: [3, 2, 1, 0]
     * which is the reverse order, Then the mapping index is: [-1, 2, -1, 0] which will mapping $3 to $0 in the path
     * layout, mapping $1 to $2, for other field not exists in the referring indices, will map to -1 which
     * means the field not exists.You can also see {@link FieldAlignPath} for more information.
     */
    private int[] createMappingIndices(int[] refPathIndices, int totalPathField) {
        if (refPathIndices.length == 0) {
            return new int[0];
        }
        int[] mapping = new int[refPathIndices[refPathIndices.length - 1] + 1];
        Arrays.fill(mapping, -1);
        for (int i = 0; i < refPathIndices.length; i++) {
            mapping[refPathIndices[i]] = totalPathField - refPathIndices[i] - 1;
        }
        return mapping;
    }

    @Override
    public ITreePath mapTree(PathMapFunction<Path> mapFunction) {
        List<Path> mapPaths = map(mapFunction);
        return createTreePath(mapPaths);
    }

    @Override
    public List<Path> toList() {
        final List<Path> pathList = new ArrayList<>();
        walkTree(paths -> {
            pathList.addAll(paths);
            return true;
        });
        return pathList;
    }

    @Override
    public List<Path> select(int... pathIndices) {
        if (ArrayUtil.isEmpty(pathIndices)) {
            return new ArrayList<>();
        }
        int minIndex = pathIndices[0];
        int maxIndex = pathIndices[0];
        for (int index : pathIndices) {
            if (index < minIndex) {
                minIndex = index;
            }
            if (index > maxIndex) {
                maxIndex = index;
            }
        }
        //if select index is out boundary return empty list
        if (maxIndex >= getDepth()) {
            return new ArrayList<>();
        }
        int maxDepth = getDepth() - minIndex;
        // 调整原来的index
        List<Integer> newIndices = new ArrayList<>();
        for (int index : pathIndices) {
            newIndices.add(index - minIndex);
        }

        Set<Path> selectPaths = new HashSet<>();
        walkTree(paths -> {
            for (Path path : paths) {
                selectPaths.add(path.subPath(newIndices));
            }
            return true;
        }, maxDepth);

        return Lists.newArrayList(selectPaths);
    }

    @Override
    public ITreePath subPath(int... pathIndices) {
        List<Path> paths = select(pathIndices);
        return TreePaths.createTreePath(paths);
    }

    public int getDepth() {
        if (getParents().isEmpty()) {
            return 1;
        }
        return getParents().get(0).getDepth() + 1;
    }

    protected void walkTree(WalkFunction walkFunction) {
        walkTree(walkFunction, -1);
    }

    protected void walkTree(WalkFunction walkFunction, int maxDepth) {
        walkTree(new ArrayList<>(), walkFunction, maxDepth, new PathIdCounter());
    }

    @SuppressWarnings("unchecked")
    public boolean walkTree(List<Object> pathNodes, WalkFunction walkFunction, int maxDepth, PathIdCounter pathId) {
        boolean isContinue = true;
        switch (getNodeType()) {
            case VERTEX_TREE:
                pathNodes.add(getVertex());
                break;
            case EDGE_TREE:
                pathNodes.add(getEdgeSet());
                break;
            default:
                throw new IllegalArgumentException("Cannot walk on this kind of tree:" + getNodeType());
        }
        // Reach the last node
        if (getParents().isEmpty() || pathNodes.size() == maxDepth) {
            List<Path> paths = new ArrayList<>();
            paths.add(new DefaultPath());

            for (int i = pathNodes.size() - 1; i >= 0; i--) {
                Object pathNode = pathNodes.get(i);
                if (pathNode instanceof RowVertex) {
                    for (Path path : paths) {
                        path.addNode((Row) pathNode);
                    }
                } else if (pathNode instanceof EdgeSet) {
                    EdgeSet edgeSet = (EdgeSet) pathNode;
                    List<Path> newPaths = new ArrayList<>(paths.size() * edgeSet.size());
                    for (Path path : paths) {
                        for (RowEdge edge : edgeSet) {
                            Path newPath = path.copy();
                            newPath.addNode(edge);
                            newPaths.add(newPath);
                        }
                    }
                    paths = newPaths;
                } else if (pathNode == null) {
                    for (Path path : paths) {
                        path.addNode(null);
                    }
                } else {
                    throw new IllegalArgumentException("Illegal path node: " + pathNode);
                }
            }
            // set id to the path.
            for (Path path : paths) {
                path.setId(pathId.getAndInc());
            }
            isContinue = walkFunction.onWalk(paths);
        } else {
            for (ITreePath parent : getParents()) {
                isContinue = parent.walkTree(pathNodes, walkFunction, maxDepth, pathId);
                if (!isContinue) {
                    break;
                }
            }
        }

        pathNodes.remove(pathNodes.size() - 1);
        return isContinue;
    }

    @Override
    public void setRequestIdForTree(Object requestId) {
        for (ITreePath parent : getParents()) {
            parent.setRequestIdForTree(requestId);
        }
        setRequestId(requestId);
    }

    @Override
    public MessageType getType() {
        return MessageType.PATH;
    }

    @Override
    public IMessage combine(IMessage other) {
        if (this.getNodeType() == NodeType.EMPTY_TREE) {
            return other;
        }
        if (((ITreePath) other).getNodeType() == NodeType.EMPTY_TREE) {
            return this;
        }
        List<ITreePath> nodes = new ArrayList<>();
        if (this instanceof UnionTreePath) {
            nodes.addAll(((UnionTreePath) this).getNodes());
        } else {
            nodes.add(this);
        }
        if (other instanceof UnionTreePath) {
            nodes.addAll(((UnionTreePath) other).getNodes());
        } else {
            nodes.add((ITreePath) other);
        }
        return UnionTreePath.create(nodes);
    }

    @Override
    public ITreePath getMessageByRequestId(Object requestId) {
        return this.getTreePath(requestId);
    }

    @Override
    public <O> List<O> map(PathMapFunction<O> mapFunction) {
        List<Path> paths = toList();
        List<O> results = new ArrayList<>();
        for (Path path : paths) {
            O result = mapFunction.map(path);
            results.add(result);
        }
        return results;
    }

    @Override
    public <O> List<O> flatMap(PathFlatMapFunction<O> flatMapFunction) {
        List<Path> paths = toList();
        List<O> finalResults = new ArrayList<>();
        for (Path path : paths) {
            Collection<O> results = flatMapFunction.flatMap(path);
            finalResults.addAll(results);
        }
        return finalResults;
    }

    @Override
    public ITreePath extendTo(RowEdge edge) {
        return extendTo(null, Lists.newArrayList(edge));
    }

    @Override
    public ITreePath extendTo(RowVertex vertex) {
        return extendTo(null, vertex);
    }
}
