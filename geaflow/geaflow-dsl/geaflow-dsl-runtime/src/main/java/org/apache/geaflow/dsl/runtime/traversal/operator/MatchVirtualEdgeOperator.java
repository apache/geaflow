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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunction;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;

public class MatchVirtualEdgeOperator extends AbstractStepOperator<MatchVirtualEdgeFunction,
    VertexRecord, EdgeGroupRecord> {

    public MatchVirtualEdgeOperator(long id, MatchVirtualEdgeFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(VertexRecord vertexRecord) {
        ITreePath treePath = vertexRecord.getTreePath();
        if (treePath != null) {
            List<Object> targetIds = treePath.flatMap(function::computeTargetId);
            if (targetIds != null) {
                Map<Object, ITreePath> targetIdPaths = new HashMap<>();
                List<RowEdge> edges = new ArrayList<>(targetIds.size());
                for (Object targetId : targetIds) {
                    RowEdge edge = new ObjectEdge(vertexRecord.getVertex().getId(), targetId);
                    edges.add(edge);

                    ITreePath targetPath = function.computeTargetPath(targetId, treePath);
                    if (targetPath != null && !treePath.isEmpty()) {
                        targetIdPaths.put(targetId, targetPath);
                    }
                    EdgeGroup edgeGroup = EdgeGroup.of(edges);
                    EdgeGroupRecord edgeGroupRecord = EdgeGroupRecord.of(edgeGroup, targetIdPaths);
                    collect(edgeGroupRecord);
                }
            }
        }
    }

    @Override
    public StepOperator<VertexRecord, EdgeGroupRecord> copyInternal() {
        return new MatchVirtualEdgeOperator(id, function);
    }
}
