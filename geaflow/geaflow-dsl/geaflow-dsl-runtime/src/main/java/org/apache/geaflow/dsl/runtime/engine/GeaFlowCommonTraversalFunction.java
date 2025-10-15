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

package org.apache.geaflow.dsl.runtime.engine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.BroadcastId;
import org.apache.geaflow.dsl.runtime.traversal.data.InitParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.TraversalAll;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageBox;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;
import org.apache.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import org.apache.geaflow.model.traversal.ITraversalRequest;

public class GeaFlowCommonTraversalFunction {

    private final ExecuteDagGroup executeDagGroup;

    private TraversalRuntimeContext context;

    private final boolean isTraversalAllWithRequest;

    private final List<ITraversalRequest<Object>> initRequests = new ArrayList<>();

    public GeaFlowCommonTraversalFunction(ExecuteDagGroup executeDagGroup, boolean isTraversalAllWithRequest) {
        this.executeDagGroup = Objects.requireNonNull(executeDagGroup);
        this.isTraversalAllWithRequest = isTraversalAllWithRequest;
    }

    public void open(TraversalRuntimeContext context) {
        this.context = Objects.requireNonNull(context);
        this.executeDagGroup.open(context);
    }

    public void init(ITraversalRequest<Object> traversalRequest) {
        initRequests.add(traversalRequest);
    }

    public void compute(Object vertexId, Iterator<MessageBox> messageIterator) {
        // Only one MessageBox in the iterator as we will combine the message in MessageCombineFunction.
        MessageBox messageBox = messageIterator.next();
        if (vertexId instanceof BroadcastId) {
            executeDagGroup.processBroadcast(messageBox);
        } else {
            context.setMessageBox(messageBox);
            long[] receiveOpIds = messageBox.getReceiverIds();
            executeDagGroup.execute(vertexId, receiveOpIds);
        }
    }

    public void finish(long iterationId) {
        if (isTraversalAllWithRequest && initRequests.size() > 0) {
            try (CloseableIterator<Object> idIterator = context.loadAllVertex()) {
                while (idIterator.hasNext()) {
                    Object vertexId = idIterator.next();
                    MessageBox messageBox = MessageType.PARAMETER_REQUEST.createMessageBox();
                    ParameterRequestMessage parameterMessage = new ParameterRequestMessage();

                    for (ITraversalRequest<Object> request : initRequests) {
                        assert Objects.equals(request.getVId(), TraversalAll.INSTANCE);
                        assert request instanceof InitParameterRequest;
                        InitParameterRequest initRequest = (InitParameterRequest) request;
                        // convert InitParameterRequest to ParameterRequest because ParameterRequest
                        // can support multi-key request id, however ITraversalRequest can only support
                        // Long type which is not enough for complex query, e.g. sub query request.
                        ParameterRequest parameterRequest = new ParameterRequest(initRequest.getRequestId(),
                            vertexId, initRequest.getParameters());
                        parameterMessage.addRequest(parameterRequest);
                    }
                    messageBox.addMessage(executeDagGroup.getEntryOpId(), parameterMessage);
                    context.setMessageBox(messageBox);
                    executeDagGroup.execute(vertexId, executeDagGroup.getEntryOpId());
                }
            } catch (Exception e) {
                throw new GeaflowRuntimeException(e);
            }
        } else {
            for (ITraversalRequest<Object> request : initRequests) {
                Object vertexId = request.getVId();
                if (request instanceof InitParameterRequest) {
                    InitParameterRequest initRequest = (InitParameterRequest) request;
                    MessageBox messageBox = MessageType.PARAMETER_REQUEST.createMessageBox();
                    ParameterRequestMessage parameterMessage = new ParameterRequestMessage();

                    // convert InitParameterRequest to ParameterRequest because ParameterRequest
                    // can support multi-key request id, however ITraversalRequest can only support
                    // Long type which is not enough for complex query, e.g. sub query request.
                    ParameterRequest parameterRequest = new ParameterRequest(initRequest.getRequestId(),
                        vertexId, initRequest.getParameters());
                    parameterMessage.addRequest(parameterRequest);
                    messageBox.addMessage(executeDagGroup.getEntryOpId(), parameterMessage);
                    context.setMessageBox(messageBox);
                } else {
                    context.setMessageBox(null);
                }
                executeDagGroup.execute(vertexId, executeDagGroup.getEntryOpId());
            }
        }

        initRequests.clear();
        executeDagGroup.finishIteration(iterationId);
    }

    public void close() {
        executeDagGroup.close();
    }

    public ExecuteDagGroup getExecuteDagGroup() {
        return executeDagGroup;
    }

    public TraversalRuntimeContext getContext() {
        return context;
    }

    public List<ITraversalRequest<Object>> getInitRequests() {
        return initRequests;
    }
}
