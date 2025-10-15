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

import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;

public class StepPathPruneCollector<OUT extends StepRecord> implements StepCollector<OUT> {

    private final TraversalRuntimeContext context;

    private final StepCollector<OUT> baseCollector;

    private final int[] outputPathFieldIndices;

    public StepPathPruneCollector(TraversalRuntimeContext context,
                                  StepCollector<OUT> baseCollector, int[] outputPathFieldIndices) {
        this.context = context;
        this.baseCollector = baseCollector;
        this.outputPathFieldIndices = outputPathFieldIndices;
    }

    @Override
    public void collect(OUT record) {
        if (record instanceof StepRecordWithPath) {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            StepRecordWithPath prunePathRecord = recordWithPath.subPathSet(outputPathFieldIndices);
            baseCollector.collect((OUT) prunePathRecord);
        } else {
            baseCollector.collect(record);
        }
    }
}
