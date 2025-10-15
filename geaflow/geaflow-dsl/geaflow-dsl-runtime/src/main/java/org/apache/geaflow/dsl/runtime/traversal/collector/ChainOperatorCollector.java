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

package org.apache.geaflow.dsl.runtime.traversal.collector;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;

public class ChainOperatorCollector implements StepCollector<StepRecord> {

    private final List<StepOperator<StepRecord, StepRecord>> nextOperators;

    private final TraversalRuntimeContext context;

    public ChainOperatorCollector(List<StepOperator<StepRecord, StepRecord>> nextOperators,
                                  TraversalRuntimeContext context) {
        this.nextOperators = nextOperators;
        this.context = context;
    }

    public ChainOperatorCollector(StepOperator<StepRecord, StepRecord> nextOperator,
                                  TraversalRuntimeContext context) {
        this(Collections.singletonList(nextOperator), context);
    }

    @Override
    public void collect(StepRecord record) {
        if (record.getType() == StepRecordType.EOD) {
            for (StepOperator<StepRecord, StepRecord> nextOperator : nextOperators) {
                nextOperator.process(record);
            }
        } else {
            for (StepOperator<StepRecord, StepRecord> nextOperator : nextOperators) {
                nextOperator.process(record);
            }
        }
    }
}
