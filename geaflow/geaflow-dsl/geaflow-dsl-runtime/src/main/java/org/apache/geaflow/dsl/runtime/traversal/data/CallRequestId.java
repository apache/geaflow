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

import java.io.Serializable;
import java.util.Objects;

public class CallRequestId implements Serializable {

    /**
     * The request id for sub query calling. It's the path id for each request path.
     */
    private final long pathId;

    private final long callOpId;

    private final Object vertexId;

    public CallRequestId(long pathId, long callOpId, Object vertexId) {
        if (pathId < 0) {
            throw new IllegalArgumentException("Illegal pathId: " + pathId);
        }
        this.pathId = pathId;
        this.callOpId = callOpId;
        this.vertexId = vertexId;
    }

    public long getPathId() {
        return pathId;
    }

    public long getCallOpId() {
        return callOpId;
    }

    public Object getVertexId() {
        return vertexId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CallRequestId)) {
            return false;
        }
        CallRequestId that = (CallRequestId) o;
        return pathId == that.pathId && callOpId == that.callOpId && vertexId.equals(that.vertexId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathId, callOpId, vertexId);
    }

    @Override
    public String toString() {
        return "CallRequestId{"
            + "pathId=" + pathId
            + ", callOpId=" + callOpId
            + ", vertexId=" + vertexId
            + '}';
    }
}
