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

import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.StepKeyRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.message.EODMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.IMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.JoinPathMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.KeyGroupMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.KeyGroupMessageImpl;
import org.apache.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepJoinOperator;
import org.apache.geaflow.dsl.runtime.traversal.path.EmptyTreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;

public class StepNextCollector implements StepCollector<StepRecord> {

    private final long senderId;

    /**
     * The op id of the receiver.
     */
    private final long receiverOpId;

    private final TraversalRuntimeContext context;

    private final boolean nextIsJoin;

    public StepNextCollector(long senderId, long receiverOpId,
                             TraversalRuntimeContext context) {
        this.senderId = senderId;
        this.receiverOpId = receiverOpId;
        this.context = context;
        this.nextIsJoin = context.getTopology().getOperator(receiverOpId)
            instanceof StepJoinOperator;
    }

    @Override
    public void collect(StepRecord record) {
        switch (record.getType()) {
            case VERTEX:
                VertexRecord vertexRecord = (VertexRecord) record;
                sendPathMessage(vertexRecord.getVertex().getId(), vertexRecord.getTreePath());
                break;
            case EDGE_GROUP:
                EdgeGroupRecord edgeGroupRecord = (EdgeGroupRecord) record;
                Set<Object> targetIds = new HashSet<>();
                for (RowEdge edge : edgeGroupRecord.getEdgeGroup()) {
                    targetIds.add(edge.getTargetId());
                }
                for (Object targetId : targetIds) {
                    sendPathMessage(targetId, edgeGroupRecord.getPathById(targetId));
                    sendRequest(targetId);
                }
                break;
            case EOD:
                EndOfData eod = (EndOfData) record;
                // broadcast EOD to all the tasks.
                context.broadcast(EODMessage.of(eod), receiverOpId);
                break;
            case KEY_RECORD:
                StepKeyRecord keyRecord = (StepKeyRecord) record;
                RowKey rowKey = keyRecord.getKey();
                KeyGroupMessage keyGroupMessage = new KeyGroupMessageImpl(Lists.newArrayList(keyRecord.getValue()));
                context.sendMessage(rowKey, keyGroupMessage, receiverOpId);
                sendRequest(rowKey);
                break;
            default:
                throw new IllegalArgumentException("Illegal record type: " + record.getType());
        }
    }

    /**
     * Send path messages to target vertex id.
     *
     * @param targetId The target vertex id.
     * @param treePath The path to send.
     */
    private void sendPathMessage(Object targetId, ITreePath treePath) {
        if (treePath == null) {
            treePath = EmptyTreePath.INSTANCE;
        }
        if (context.getRequest() != null) {
            // set requestId for tree path.
            treePath.setRequestIdForTree(context.getRequest().getRequestId());
        }
        IMessage pathMessage;
        if (nextIsJoin) { // If next op is join, add the senderId to the message.
            pathMessage = JoinPathMessage.from(senderId, treePath);
        } else {
            pathMessage = treePath;
        }
        // Send path message.
        context.sendMessage(targetId, pathMessage, receiverOpId);
    }

    private void sendRequest(Object targetId) {
        // Send request message.
        if (context.getRequest() != null) {
            ParameterRequest request = context.getRequest();
            ParameterRequestMessage requestMessage = new ParameterRequestMessage();
            requestMessage.addRequest(request);
            context.sendMessage(targetId, requestMessage, receiverOpId);
        }
    }
}
