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

import org.apache.geaflow.model.record.RecordArgs;
import org.apache.geaflow.shuffle.serialize.IMessageIterator;

public class PipelineMessage<T> implements PipelineEvent {

    private final int edgeId;
    private final RecordArgs recordArgs;
    private final IMessageIterator<T> messageIterator;

    public PipelineMessage(int edgeId, long batchId, String streamName, IMessageIterator<T> messageIterator) {
        this.edgeId = edgeId;
        this.recordArgs = new RecordArgs(batchId, streamName);
        this.messageIterator = messageIterator;
    }

    @Override
    public int getEdgeId() {
        return this.edgeId;
    }

    @Override
    public long getWindowId() {
        return recordArgs.getWindowId();
    }

    public IMessageIterator<T> getMessageIterator() {
        return messageIterator;
    }

    public RecordArgs getRecordArgs() {
        return recordArgs;
    }

}
