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
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath;

public class StepEndCollector implements StepCollector<StepRecord> {

    public static final String TRAVERSAL_FINISH = "TraversalFinish";

    private final TraversalRuntimeContext context;

    public StepEndCollector(TraversalRuntimeContext context) {
        this.context = context;
    }

    @Override
    public void collect(StepRecord record) {
        if (record.getType() == StepRecordType.EOD) {
            // The StepEndOperator has received all the EOD, send TRAVERSAL_FINISH to the coordinator
            // to finish the iteration.
            context.sendCoordinator(TRAVERSAL_FINISH, 1);
        } else {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            for (ITreePath path : recordWithPath.getPaths()) {
                if (context.getParameters() != null) {
                    // If current is request with parameter, carry the request id and request parameter out.
                    path = new ParameterizedTreePath(path, context.getRequest().getRequestId(), context.getParameters());
                }
                context.takePath(path);
            }
        }
    }
}
