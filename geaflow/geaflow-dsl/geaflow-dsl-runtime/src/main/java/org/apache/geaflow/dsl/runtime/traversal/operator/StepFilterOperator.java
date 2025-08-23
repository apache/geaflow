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

import org.apache.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import org.apache.geaflow.dsl.runtime.util.StepFunctionUtil;

public class StepFilterOperator extends AbstractStepOperator<StepBoolFunction, StepRecordWithPath, StepRecordWithPath> {

    private final int[] refPathIndices;

    public StepFilterOperator(long id, StepBoolFunction function) {
        super(id, function);
        this.refPathIndices = StepFunctionUtil.getRefPathIndices(function);
    }

    @Override
    public void processRecord(StepRecordWithPath record) {
        StepRecordWithPath filterRecord = record.filter(path -> function.filter(withParameter(path)), refPathIndices);
        if (!filterRecord.isPathEmpty()) {
            collect(filterRecord);
        }
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepFilterOperator(id, function);
    }
}
