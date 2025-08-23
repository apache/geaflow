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

import java.util.List;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.runtime.function.graph.StepMapRowFunction;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;

public class StepMapRowOperator extends AbstractStepOperator<StepMapRowFunction, StepRecord, Row> {

    public StepMapRowOperator(long id, StepMapRowFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.ROW) {
            Row result = function.map((Row) record);
            collect(result);
        } else {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            List<Row> rows = recordWithPath.map(path -> function.map(withParameter(path)), null);
            if (rows != null) {
                for (Row row : rows) {
                    collect(row);
                }
            }
        }
    }

    @Override
    public StepOperator<StepRecord, Row> copyInternal() {
        return new StepMapRowOperator(id, function);
    }
}
