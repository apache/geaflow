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

package org.apache.geaflow.shuffle.pipeline.buffer;

import java.io.Serializable;

public class PipeBuffer implements Serializable {

    private final OutBuffer buffer;
    private final boolean isData;
    private final long batchId;
    private final int count;
    private final boolean isFinish;

    public PipeBuffer(byte[] buffer, long batchId) {
        this(new HeapBuffer(buffer), batchId);
    }

    public PipeBuffer(OutBuffer buffer, long batchId) {
        this.buffer = buffer;
        this.batchId = batchId;
        this.isData = true;
        this.count = 0;
        this.isFinish = false;
    }

    public PipeBuffer(long batchId, int count, boolean isFinish) {
        this.buffer = null;
        this.batchId = batchId;
        this.isData = false;
        this.count = count;
        this.isFinish = isFinish;
    }

    public OutBuffer getBuffer() {
        return buffer;
    }

    public int getBufferSize() {
        return buffer != null ? buffer.getBufferSize() : 0;
    }

    public boolean isData() {
        return isData;
    }

    public long getBatchId() {
        return batchId;
    }

    public int getCount() {
        return count;
    }

    public boolean isFinish() {
        return isFinish;
    }
}
