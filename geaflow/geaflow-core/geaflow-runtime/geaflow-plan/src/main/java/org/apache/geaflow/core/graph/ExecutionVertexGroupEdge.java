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

package org.apache.geaflow.core.graph;

import java.io.Serializable;
import org.apache.geaflow.partitioner.IPartitioner;

public class ExecutionVertexGroupEdge implements Serializable {

    private int groupEdgeId;
    private int groupSrcId;
    private int groupTargetId;
    private IPartitioner partitioner;
    private String groupEdgeName;

    public ExecutionVertexGroupEdge(IPartitioner partitioner, int groupEdgeId,
                                    String groupEdgeName, int groupSrcId, int groupTargetId) {
        this.partitioner = partitioner;
        this.groupEdgeId = groupEdgeId;
        this.groupEdgeName = groupEdgeName;
        this.groupSrcId = groupSrcId;
        this.groupTargetId = groupTargetId;
    }

    public int getGroupEdgeId() {
        return groupEdgeId;
    }

    public void setGroupEdgeId(int groupEdgeId) {
        this.groupEdgeId = groupEdgeId;
    }

    public int getGroupSrcId() {
        return groupSrcId;
    }

    public void setGroupSrcId(int groupSrcId) {
        this.groupSrcId = groupSrcId;
    }

    public int getGroupTargetId() {
        return groupTargetId;
    }

    public void setGroupTargetId(int groupTargetId) {
        this.groupTargetId = groupTargetId;
    }

    public IPartitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(IPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public String getGroupEdgeName() {
        return groupEdgeName;
    }

    public void setGroupEdgeName(String groupEdgeName) {
        this.groupEdgeName = groupEdgeName;
    }
}
