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

package org.apache.geaflow.common.visualization.console;

import java.util.ArrayList;
import java.util.List;

public class ConsoleVisualizeVertex {

    public String vertexType;
    public String vertexMode;
    public String id;
    public int parallelism;
    public String operator;
    public String operatorName;
    public List<Predecessor> parents = new ArrayList<>();
    public JsonPlan innerPlan;

    public String getVertexType() {
        return vertexType;
    }

    public void setVertexType(String vertexType) {
        this.vertexType = vertexType;
    }

    public String getVertexMode() {
        return vertexMode;
    }

    public void setVertexMode(String vertexMode) {
        this.vertexMode = vertexMode;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public List<Predecessor> getParents() {
        return parents;
    }

    public void setParents(List<Predecessor> parents) {
        this.parents = parents;
    }

    public JsonPlan getInnerPlan() {
        return innerPlan;
    }

    public void setInnerPlan(JsonPlan innerPlan) {
        this.innerPlan = innerPlan;
    }
}
