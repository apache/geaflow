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

package org.apache.geaflow.shuffle.message;

import java.io.Serializable;
import java.util.Objects;

public class WriterId implements Serializable {

    private final long pipelineId;
    private final int edgeId;
    private final int shardIndex;

    public WriterId(long pipelineId, int edgeId, int shardIndex) {
        this.pipelineId = pipelineId;
        this.edgeId = edgeId;
        this.shardIndex = shardIndex;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WriterId writerID = (WriterId) o;
        return pipelineId == writerID.pipelineId && edgeId == writerID.edgeId
            && shardIndex == writerID.shardIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineId, edgeId, shardIndex);
    }

    @Override
    public String toString() {
        return "WriterId{" + "pipelineId=" + pipelineId + ", edgeId=" + edgeId + ", shardIndex="
            + shardIndex + '}';
    }

}
