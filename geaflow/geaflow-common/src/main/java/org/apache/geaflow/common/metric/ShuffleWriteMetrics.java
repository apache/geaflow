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

public class ShuffleWriteMetrics implements Serializable {

    /**
     * total output channels.
     */
    private long numChannels;
    /**
     * total output channels with data written.
     */
    private long writtenChannels;

    /**
     * total written records.
     */
    private long writtenRecords;
    /**
     * total written bytes.
     */
    private long encodedSize;
    /**
     * time cost on serializing.
     */
    private long encodeMs;
    /**
     * max written slice size in KB.
     */
    private long maxSliceKB;

    /**
     * total spills.
     */
    private int spillNum;
    /**
     * total spills to disk.
     */
    private int spillDisk;
    /**
     * total oom count.
     */
    private int oomCount;
    /**
     * max spilled size in KB.
     */
    private long maxSpillKB;

    /**
     * total spill cost in ms.
     */
    private long spillMs;
    private long flushMs;

    public void merge(ShuffleWriteMetrics metrics) {
        this.numChannels += metrics.numChannels;
        this.writtenChannels += metrics.writtenChannels;
        this.writtenRecords += metrics.writtenRecords;
        this.encodedSize += metrics.encodedSize;
        this.encodeMs += metrics.encodeMs;
        this.spillNum += metrics.spillNum;
        this.spillDisk += metrics.spillDisk;
        this.oomCount += metrics.oomCount;
        this.spillMs += metrics.spillMs;
        this.flushMs += metrics.flushMs;
        this.maxSliceKB = Math.max(maxSliceKB, metrics.maxSliceKB);
        this.maxSpillKB = Math.max(maxSpillKB, metrics.maxSpillKB);
    }

    public long getWrittenRecords() {
        return writtenRecords;
    }

    public void setWrittenRecords(long writtenRecords) {
        this.writtenRecords = writtenRecords;
    }

    public long getEncodedSize() {
        return encodedSize;
    }

    public void setEncodedSize(long encodedSize) {
        this.encodedSize = encodedSize;
    }

    public long getEncodeMs() {
        return encodeMs;
    }

    public void setEncodeMs(long encodeMs) {
        this.encodeMs = encodeMs;
    }

    public int getSpillNum() {
        return spillNum;
    }

    public void setSpillNum(int spillNum) {
        this.spillNum = spillNum;
    }

    public int getSpillDisk() {
        return spillDisk;
    }

    public void setSpillDisk(int spillDisk) {
        this.spillDisk = spillDisk;
    }

    public int getOomCount() {
        return oomCount;
    }

    public void setOomCount(int oomCount) {
        this.oomCount = oomCount;
    }

    public long getMaxSpillKB() {
        return maxSpillKB;
    }

    public void setMaxSpillKB(long maxSpillKB) {
        this.maxSpillKB = maxSpillKB;
    }

    public long getMaxSliceKB() {
        return maxSliceKB;
    }

    public void setMaxSliceKB(long maxSliceSizeKB) {
        this.maxSliceKB = maxSliceSizeKB;
    }

    public long getSpillMs() {
        return spillMs;
    }

    public void setSpillMs(long spillMs) {
        this.spillMs = spillMs;
    }

    public long getNumChannels() {
        return numChannels;
    }

    public void setNumChannels(long numChannels) {
        this.numChannels = numChannels;
    }

    public long getWrittenChannels() {
        return writtenChannels;
    }

    public void setWrittenChannels(long writtenChannels) {
        this.writtenChannels = writtenChannels;
    }

    public long getFlushMs() {
        return flushMs;
    }

    public void setFlushMs(long flushMs) {
        this.flushMs = flushMs;
    }

    public void increaseRecords(long recordNum) {
        this.writtenRecords += recordNum;
    }

    public void increaseEncodedSize(long bytes) {
        this.encodedSize += bytes;
    }

    public void increaseEncodeMs(long encodeMs) {
        this.encodeMs += encodeMs;
    }

    public void increaseSpillMs(long spillMs) {
        this.spillMs += spillMs;
    }

    public void increaseSpillNum() {
        this.spillNum++;
    }

    public void increaseWrittenChannels() {
        writtenChannels++;
    }

    public void updateMaxSpillKB(long spillKB) {
        if (this.maxSpillKB < spillKB) {
            this.maxSpillKB = spillKB;
        }
    }

    @Override
    public String toString() {
        return "WriteMetrics{" + "outputRecords=" + writtenRecords + ", encodedKb="
            + encodedSize / 1024 + ", encodeMs=" + encodeMs + ", spillNum=" + spillNum
            + ", spillDisk=" + spillDisk + ", oomCnt=" + oomCount + ", spillMs=" + spillMs
            + ", maxSpillKB=" + maxSpillKB + ", " + "maxSliceKB=" + maxSliceKB + ", channels="
            + numChannels + ", writtenChannels=" + writtenChannels + '}';
    }

}
