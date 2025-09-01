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

package org.apache.geaflow.dsl.common.compile;

import java.io.Serializable;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.common.visualization.console.JsonPlan;

public class CompileResult implements Serializable {

    private JsonPlan physicPlan;

    private Set<TableInfo> sourceTables;

    private Set<TableInfo> targetTables;

    private Set<GraphInfo> sourceGraphs;

    private Set<GraphInfo> targetGraphs;

    private RelDataType currentResultType;

    public CompileResult() {

    }

    public RelDataType getCurrentResultType() {
        return currentResultType;
    }

    public void setCurrentResultType(RelDataType currentResultType) {
        this.currentResultType = currentResultType;
    }

    public JsonPlan getPhysicPlan() {
        return physicPlan;
    }

    public void setPhysicPlan(JsonPlan physicPlan) {
        this.physicPlan = physicPlan;
    }

    public Set<TableInfo> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(Set<TableInfo> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public Set<TableInfo> getTargetTables() {
        return targetTables;
    }

    public void setTargetTables(Set<TableInfo> targetTables) {
        this.targetTables = targetTables;
    }

    public Set<GraphInfo> getSourceGraphs() {
        return sourceGraphs;
    }

    public void setSourceGraphs(Set<GraphInfo> sourceGraphs) {
        this.sourceGraphs = sourceGraphs;
    }

    public Set<GraphInfo> getTargetGraphs() {
        return targetGraphs;
    }

    public void setTargetGraphs(Set<GraphInfo> targetGraphs) {
        this.targetGraphs = targetGraphs;
    }
}
