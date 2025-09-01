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

import java.util.HashSet;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;

public class StepDistinctOperator extends AbstractStepOperator<StepKeyFunction, StepRecordWithPath, StepRecordWithPath> {

    private final Set<RowKey> distinctKeys = new HashSet<>();

    private int[] refPathIndices;

    public StepDistinctOperator(long id, StepKeyFunction function) {
        super(id, function);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        assert inputPathSchemas.size() == 1;
        refPathIndices = new int[inputPathSchemas.get(0).size()];
        // refer all the input path fields
        for (int i = 0; i < refPathIndices.length; i++) {
            refPathIndices[i] = i;
        }
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        StepRecordWithPath distinctRecord = record.filter(path -> {
            RowKey key = function.getKey(path);
            if (!distinctKeys.contains(key)) {
                distinctKeys.add(key);
                return true;
            }
            return false;
        }, refPathIndices);

        collect(distinctRecord);
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepDistinctOperator(id, function);
    }

    @Override
    public void finish() {
        distinctKeys.clear();
        super.finish();
    }
}
