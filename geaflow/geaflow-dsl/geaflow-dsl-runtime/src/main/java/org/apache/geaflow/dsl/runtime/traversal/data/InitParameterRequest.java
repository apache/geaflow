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

import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.TraversalType.RequestType;

public class InitParameterRequest implements ITraversalRequest<Object> {

    private final long requestId;

    private final Object vertexId;

    private final Row parameters;

    public InitParameterRequest(long requestId, Object vertexId, Row parameters) {
        this.requestId = requestId;
        this.vertexId = vertexId;
        this.parameters = parameters;
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public Object getVId() {
        return vertexId;
    }

    @Override
    public RequestType getType() {
        return RequestType.Vertex;
    }

    public Row getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "InitParameterRequest{"
            + "requestId=" + requestId
            + ", vertexId=" + vertexId
            + ", parameters=" + parameters
            + '}';
    }
}
