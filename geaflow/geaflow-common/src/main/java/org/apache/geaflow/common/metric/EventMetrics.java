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

package org.apache.geaflow.common.metric;

import java.io.Serializable;
import org.apache.geaflow.common.utils.GcUtil;

public class EventMetrics implements Serializable {

    /**
     * Meta.
     */
    private final int vertexId;
    private final int parallelism;
    private final int index;

    /**
     * Execution.
     */
    private long startTime;
    private long finishTime;
    private long executeCostMs;
    private long processCostMs;
    private long gcCostMs;

    /**
     * Shuffle.
     */
    private long shuffleReadRecords;
    private long shuffleReadBytes;
    private long shuffleReadCostMs;

    private long shuffleWriteRecords;
    private long shuffleWriteBytes;
    private long shuffleWriteCostMs;

    private transient long startGcTs;

    public EventMetrics(int vertexId, int parallelism, int index) {
        this.vertexId = vertexId;
        this.parallelism = parallelism;
        this.index = index;
        this.startTime = System.currentTimeMillis();
        this.startGcTs = GcUtil.computeCurrentTotalGcTime();
    }

    public int getVertexId() {
        return this.vertexId;
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public int getIndex() {
        return this.index;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
        this.executeCostMs = this.finishTime - this.startTime;
    }

    public long getExecuteCostMs() {
        return this.executeCostMs;
    }

    public long getProcessCostMs() {
        return this.processCostMs;
    }

    public void setProcessCostMs(long processCostMs) {
        this.processCostMs = processCostMs;
    }

    public void addProcessCostMs(long processCostMs) {
        this.processCostMs += processCostMs;
    }

    public long getGcCostMs() {
        return this.gcCostMs;
    }

    public long getShuffleReadRecords() {
        return this.shuffleReadRecords;
    }

    public void setShuffleReadRecords(long shuffleReadRecords) {
        this.shuffleReadRecords = shuffleReadRecords;
    }

    public void addShuffleReadRecords(long shuffleReadRecords) {
        this.shuffleReadRecords += shuffleReadRecords;
    }

    public long getShuffleReadBytes() {
        return this.shuffleReadBytes;
    }

    public void setShuffleReadBytes(long shuffleReadBytes) {
        this.shuffleReadBytes = shuffleReadBytes;
    }

    public void addShuffleReadBytes(long shuffleReadBytes) {
        this.shuffleReadBytes += shuffleReadBytes;
    }

    public long getShuffleReadCostMs() {
        return this.shuffleReadCostMs;
    }

    public void setShuffleReadCostMs(long shuffleReadCostMs) {
        this.shuffleReadCostMs = shuffleReadCostMs;
    }

    public void addShuffleReadCostMs(long shuffleReadCostMs) {
        this.shuffleReadCostMs += shuffleReadCostMs;
    }

    public long getShuffleWriteRecords() {
        return this.shuffleWriteRecords;
    }

    public void setShuffleWriteRecords(long shuffleWriteRecords) {
        this.shuffleWriteRecords = shuffleWriteRecords;
    }

    public void addShuffleWriteRecords(long shuffleWriteRecords) {
        this.shuffleWriteRecords += shuffleWriteRecords;
    }

    public long getShuffleWriteBytes() {
        return this.shuffleWriteBytes;
    }

    public void setShuffleWriteBytes(long shuffleWriteBytes) {
        this.shuffleWriteBytes = shuffleWriteBytes;
    }

    public void addShuffleWriteBytes(long shuffleWriteBytes) {
        this.shuffleWriteBytes += shuffleWriteBytes;
    }

    public long getShuffleWriteCostMs() {
        return this.shuffleWriteCostMs;
    }

    public void addShuffleWriteCostMs(long shuffleWriteCostMs) {
        this.shuffleWriteCostMs += shuffleWriteCostMs;
    }

    public void setStartGcTs(long startGcTs) {
        this.startGcTs = startGcTs;
    }

    public void setFinishGcTs(long finishGcTs) {
        this.gcCostMs = finishGcTs - this.startGcTs;
    }

    @Override
    public String toString() {
        return "EventMetrics{"
            + "startTime=" + startTime
            + ", finishTime=" + finishTime
            + ", executeCostMs=" + executeCostMs
            + ", processCostMs=" + processCostMs
            + ", gcCostMs=" + gcCostMs
            + ", shuffleReadRecords=" + shuffleReadRecords
            + ", shuffleReadBytes=" + shuffleReadBytes
            + ", shuffleReadCostMs=" + shuffleReadCostMs
            + ", shuffleWriteRecords=" + shuffleWriteRecords
            + ", shuffleWriteBytes=" + shuffleWriteBytes
            + ", shuffleWriteCostMs=" + shuffleWriteCostMs
            + '}';
    }

}
