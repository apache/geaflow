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
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.VirtualId;
import org.apache.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepJumpCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.state.pushdown.filter.EmptyFilter;

public class StepLoopUntilOperator extends AbstractStepOperator<StepBoolFunction,
    StepRecordWithPath, StepRecordWithPath> {

    private final long loopStartOpId;
    private final long loopBodyOpId;
    private StepCollector<StepRecord> loopStartCollector;
    private int loopCounter;

    private final int minLoopCount;
    private final int maxLoopCount;

    private final int loopStartPathFieldCount;
    private final int loopBodyPathFieldCount;

    private int[] pathIndices;

    public StepLoopUntilOperator(long id, long loopStartOpId,
                                 long loopBodyOpId, StepBoolFunction function,
                                 int minLoopCount, int maxLoopCount,
                                 int loopStartPathFieldCount, int loopBodyPathFieldCount) {
        super(id, function);
        this.loopStartOpId = loopStartOpId;
        this.loopBodyOpId = loopBodyOpId;
        this.minLoopCount = minLoopCount;
        this.maxLoopCount = maxLoopCount == -1 ? Integer.MAX_VALUE : maxLoopCount;
        this.loopStartPathFieldCount = loopStartPathFieldCount;
        this.loopBodyPathFieldCount = loopBodyPathFieldCount;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.loopStartCollector = new StepJumpCollector(id, loopStartOpId, context);
        // When reach the loop-util, we have already looped 1 time, so the init loop counter should be 1.
        this.loopCounter = 1;
        this.pathIndices = new int[loopBodyPathFieldCount + loopStartPathFieldCount];
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepLoopUntilOperator(id, loopStartOpId, loopBodyOpId, function, minLoopCount, maxLoopCount,
            loopStartPathFieldCount, loopBodyPathFieldCount);
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        boolean fromZeroLoop = context.getInputOperatorId() != loopBodyOpId;
        StepRecordWithPath lastLoopPath = selectLastLoopPath(record, fromZeroLoop);
        if (fromZeroLoop) { // from the loop start (for loop 0)
            collect(lastLoopPath);
        } else { // from the loop body
            if (loopCounter >= minLoopCount) {
                collect(lastLoopPath);
            }
            if (loopCounter < maxLoopCount) {
                loopStartCollector.collect(lastLoopPath);
            }
        }
    }

    private StepRecordWithPath selectLastLoopPath(StepRecordWithPath record, boolean fromZeroLoop) {
        RowVertex vertexId = ((VertexRecord) record).getVertex();
        if (fromZeroLoop) {
            final RowVertex vertexRecord;
            if (vertexId instanceof IdOnlyVertex && !(vertexId.getId() instanceof VirtualId)) {
                vertexRecord = context.loadVertex(vertexId.getId(),
                    EmptyFilter.getInstance(),
                    graphSchema,
                    addingVertexFieldTypes);
            } else {
                vertexRecord = vertexId;
            }
            return record.mapTreePath(treePath -> {
                ITreePath newTreePath = treePath;
                for (int i = 0; i < loopBodyPathFieldCount - 1; i++) {
                    newTreePath = newTreePath.extendTo((RowEdge) null);
                }
                return newTreePath.extendTo(vertexRecord);
            });
        } else {
            for (int i = 0; i < loopStartPathFieldCount; i++) {
                pathIndices[i] = i;
            }
            for (int i = 0; i < loopBodyPathFieldCount; i++) {
                // When calculating the index for the loopBody fields, when
                // loopCounter is 1, the first offset is used for input values. After that,
                // values generated by the loop are placed starting from an offset of 1
                pathIndices[i + loopStartPathFieldCount] = loopStartPathFieldCount
                    + Math.min(loopCounter - 1, 1) * loopBodyPathFieldCount + i;
            }
            return record.subPathSet(pathIndices);
        }
    }

    @Override
    protected void onReceiveAllEOD(long callerOpId, List<EndOfData> receiveEods) {
        boolean isGlobalEmptyCycle = true;
        for (EndOfData eod : receiveEods) {
            if (eod.getSenderId() == this.loopBodyOpId) {
                isGlobalEmptyCycle &= eod.isGlobalEmptyCycle;
            }
        }
        if (loopCounter < maxLoopCount && !isGlobalEmptyCycle) {
            // remove eod from the loop body.
            receiveEods.removeIf(eod -> eod.getSenderId() == this.loopBodyOpId);
            // send EOD to the loop start.
            EndOfData eod = EndOfData.of(callerOpId, id);
            eod.isGlobalEmptyCycle = numProcessRecords == 0;
            loopStartCollector.collect(eod);
        } else { // If no data in the loop, it means the whole loop has finished. Just send EOD to the next.
            super.onReceiveAllEOD(callerOpId, receiveEods);
            receiveEods.clear();
        }
        this.isGlobalEmptyCycle = true;
        this.numProcessRecords = 0L;
        this.loopCounter++;
    }

    public int getMinLoopCount() {
        return this.minLoopCount;
    }

    public int getMaxLoopCount() {
        return this.maxLoopCount;
    }
}
