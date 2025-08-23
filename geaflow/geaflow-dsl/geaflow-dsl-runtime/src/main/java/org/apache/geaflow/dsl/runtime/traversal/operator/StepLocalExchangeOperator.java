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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.StepKeyRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.StepKeyRecordImpl;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.TreePaths;

public class StepLocalExchangeOperator extends AbstractStepOperator<StepKeyFunction, StepRecord, StepRecord> {


    public StepLocalExchangeOperator(long id, StepKeyFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.ROW) {
            Row row = (Row) record;
            RowKey key = function.getKey(row);
            if (context.getRequest() != null) { // append requestId to the key.
                key = new DefaultRowKeyWithRequestId(context.getRequestId(), key);
            }
            StepKeyRecord keyRecord = new StepKeyRecordImpl(key, row);
            collect(keyRecord);
        } else {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            for (ITreePath treePath : recordWithPath.getPaths()) {
                List<Path> paths = treePath.toList();
                for (Path path : paths) {
                    RowKey key = function.getKey(path);
                    RowVertex virtualVertex = IdOnlyVertex.of(key);
                    ITreePath virtualVertexPath = TreePaths.createTreePath(Lists.newArrayList(path));
                    collect(VertexRecord.of(virtualVertex, virtualVertexPath));
                }
            }
        }
    }

    @Override
    public StepOperator<StepRecord, StepRecord> copyInternal() {
        return new StepLocalExchangeOperator(id, function);
    }
}
