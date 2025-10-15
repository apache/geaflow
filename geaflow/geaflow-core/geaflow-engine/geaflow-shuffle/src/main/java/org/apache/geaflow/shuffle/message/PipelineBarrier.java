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

package org.apache.geaflow.shuffle.message;

import java.util.Objects;

public class PipelineBarrier implements PipelineEvent {

    // Input edge id.
    private final int edgeId;

    // Iteration id of cycle.
    private final long windowId;

    // If of source task that send the event from.
    private int sourceTaskIndex;

    // Id of target task that send the event to.
    private int targetTaskIndex;

    // Message count that current event involved.
    private long count;

    // Flag that denote source task is finished after the current event.
    private boolean finish;

    public PipelineBarrier(long windowId, int edgeId, int sourceTaskIndex) {
        this.edgeId = edgeId;
        this.windowId = windowId;
        this.sourceTaskIndex = sourceTaskIndex;
    }

    public PipelineBarrier(long windowId, int edgeId, long count) {
        this.edgeId = edgeId;
        this.windowId = windowId;
        this.count = count;
        this.finish = false;
    }

    public PipelineBarrier(long windowId, int edgeId, int sourceTaskIndex, int targetTaskId, long count) {
        this.edgeId = edgeId;
        this.windowId = windowId;
        this.sourceTaskIndex = sourceTaskIndex;
        this.targetTaskIndex = targetTaskId;
        this.count = count;
        this.finish = false;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }

    public boolean isFinish() {
        return finish;
    }

    @Override
    public long getWindowId() {
        return windowId;
    }

    public int getSourceTaskIndex() {
        return sourceTaskIndex;
    }

    public int getTargetTaskIndex() {
        return targetTaskIndex;
    }

    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelineBarrier that = (PipelineBarrier) o;
        return edgeId == that.edgeId && windowId == that.windowId
            && sourceTaskIndex == that.sourceTaskIndex && targetTaskIndex == that.targetTaskIndex
            && count == that.count && finish == that.finish;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, windowId, sourceTaskIndex, targetTaskIndex, count, finish);
    }

    @Override
    public String toString() {
        return "PipelineBarrier{" + "edgeId=" + edgeId + ", windowId=" + windowId
            + ", sourceTaskIndex=" + sourceTaskIndex + ", targetTaskIndex=" + targetTaskIndex
            + ", count=" + count + ", finish=" + finish + '}';
    }

}
