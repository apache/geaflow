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

package org.apache.geaflow.dsl.runtime.traversal.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.runtime.function.graph.StepJoinFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.message.JoinPathMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.TreePaths;
import org.apache.geaflow.dsl.runtime.util.SchemaUtil;

public class StepJoinOperator extends AbstractStepOperator<StepJoinFunction, VertexRecord, VertexRecord> {

    private final PathType inputJoinPathSchema;

    private final List<PathType> inputPathSchemas;

    // requestId -> jonKey -> (leftTreePath, rightTreePath)
    private Map<Object, Map<RowKey, JoinTree>> cachedLeftAndRightTreePaths;

    private long leftInputOpId;

    private long rightInputOpId;

    private boolean isLocalJoin;

    public StepJoinOperator(long id, StepJoinFunction function, PathType inputJoinPathSchema,
                            List<PathType> inputPathSchemas, boolean isLocalJoin) {
        super(id, function);
        this.inputJoinPathSchema = Objects.requireNonNull(inputJoinPathSchema);
        this.inputPathSchemas = Objects.requireNonNull(inputPathSchemas);
        assert inputPathSchemas.size() == 2;
        this.isLocalJoin = isLocalJoin;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.cachedLeftAndRightTreePaths = new HashMap<>();
        this.leftInputOpId = context.getTopology().getInputIds(id).get(0);
        this.rightInputOpId = context.getTopology().getInputIds(id).get(1);
    }

    @Override
    protected void processRecord(VertexRecord record) {
        RowVertex vertex = record.getVertex();
        RowKey joinKey = (RowKey) vertex.getId();
        ITreePath leftTree;
        ITreePath rightTree;
        if (isLocalJoin) {
            leftTree = context.getInputOperatorId() == leftInputOpId ? record.getTreePath() : null;
            rightTree = context.getInputOperatorId() == rightInputOpId ? record.getTreePath() : null;
        } else {
            JoinPathMessage pathMessage = context.getMessage(MessageType.JOIN_PATH);
            leftTree = pathMessage.getTreePath(leftInputOpId);
            rightTree = pathMessage.getTreePath(rightInputOpId);
        }
        cachedLeftAndRightTreePaths
            .computeIfAbsent(context.getRequestId(), k -> new HashMap<>())
            .computeIfAbsent(joinKey, k -> new JoinTree())
            .addLeftTree(leftTree)
            .addRightTree(rightTree);
    }

    @Override
    public void finish() {
        Set<Object> requestIds = cachedLeftAndRightTreePaths.keySet();
        for (Object requestId : requestIds) {
            Map<RowKey, JoinTree> joinTreeMap = cachedLeftAndRightTreePaths.get(requestId);

            for (Map.Entry<RowKey, JoinTree> entry : joinTreeMap.entrySet()) {
                RowKey joinKey = entry.getKey();
                JoinTree joinTree = entry.getValue();

                List<Path> joinPaths = new ArrayList<>();
                List<Path> leftPaths =
                    joinTree.leftTree == null ? Collections.singletonList(null) : joinTree.leftTree.toList();
                List<Path> rightPaths =
                    joinTree.rightTree == null ? Collections.singletonList(null) : joinTree.rightTree.toList();

                for (Path leftPath : leftPaths) {
                    int numJoinEdge = 0;
                    for (Path rightPath : rightPaths) {
                        Path joinPath = function.join(leftPath, rightPath);
                        if (joinPath != null) {
                            numJoinEdge++;
                            joinPaths.add(joinPath);
                        }
                    }
                    if (numJoinEdge == 0) {
                        switch (function.getJoinType()) {
                            case LEFT:
                                joinPaths.add(SchemaUtil.alignToPathSchema(leftPath,
                                    inputPathSchemas.get(0), getOutputPathSchema()));
                                break;
                            default:
                        }
                    }
                }
                for (Path joinPath : joinPaths) {
                    ITreePath joinTreePath = TreePaths.singletonPath(joinPath);
                    collect(VertexRecord.of(IdOnlyVertex.of(joinKey), joinTreePath));
                }
            }
        }
        cachedLeftAndRightTreePaths.clear();
        super.finish();
    }

    @Override
    public StepOperator<VertexRecord, VertexRecord> copyInternal() {
        return new StepJoinOperator(id, function, inputJoinPathSchema, inputPathSchemas,
            isLocalJoin);
    }

    @Override
    protected PathType concatInputPathType() {
        return inputJoinPathSchema;
    }

    private static class JoinTree {

        public ITreePath leftTree;

        public ITreePath rightTree;

        public JoinTree addLeftTree(ITreePath leftTree) {
            if (leftTree != null) {
                if (this.leftTree == null) {
                    this.leftTree = leftTree;
                } else {
                    this.leftTree = this.leftTree.merge(leftTree);
                }
            }
            return this;
        }

        public JoinTree addRightTree(ITreePath rightTree) {
            if (rightTree != null) {
                if (this.rightTree == null) {
                    this.rightTree = rightTree;
                } else {
                    this.rightTree = this.rightTree.merge(rightTree);
                }
            }
            return this;
        }
    }
}
