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
import java.util.List;
import org.apache.geaflow.dsl.runtime.function.graph.StepMapFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;

public class StepMapOperator extends AbstractStepOperator<StepMapFunction, StepRecordWithPath, StepRecordWithPath> {

    private List<StepRecordWithPath> cacheRecords;
    private final boolean isGlobal;

    public StepMapOperator(long id, StepMapFunction function, boolean isGlobal) {
        super(id, function);
        this.isGlobal = isGlobal;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        if (isGlobal) {
            cacheRecords = new ArrayList<>();
        }
    }

    @Override
    public void finish() {
        if (isGlobal) {
            for (StepRecordWithPath record : cacheRecords) {
                collect(record);
            }
            cacheRecords.clear();
        }
        super.finish();
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        StepRecordWithPath mapRecord = record.mapPath(path -> function.map(withParameter(path)),
            null);
        if (isGlobal) {
            cacheRecords.add(mapRecord);
        } else {
            collect(mapRecord);
        }
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepMapOperator(id, function, isGlobal);
    }
}
