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

package org.apache.geaflow.plan.visualization;

import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;

public class GeaFlowNodeInfo {

    private int vertexId;
    private String type;
    private OpDesc operator;
    private int parallelism;

    public GeaFlowNodeInfo(int vertexId, String type, Operator operator) {
        this.vertexId = vertexId;
        this.type = type;
        this.operator = new OpDesc(operator);
        this.parallelism = ((AbstractOperator) operator).getOpArgs().getParallelism();
    }

    public int getParallelism() {
        return parallelism;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public String toGraphvizNodeString() {
        StringBuilder builder = new StringBuilder();
        builder.append(vertexId).append(" [label=\"");
        builder.append("p:").append(parallelism);
        builder.append(", ").append(operator.getName());
        builder.append("\"]\n");
        return builder.toString();
    }
}
