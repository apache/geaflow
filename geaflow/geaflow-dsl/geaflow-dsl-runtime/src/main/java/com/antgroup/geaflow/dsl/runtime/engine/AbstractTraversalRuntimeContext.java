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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction.VertexCentricAggContext;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallContext;
import com.antgroup.geaflow.dsl.runtime.traversal.DagTopologyGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.BroadcastId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.CallRequestId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.message.IMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.RequestIsolationMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.util.IDUtil;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

public abstract class AbstractTraversalRuntimeContext implements TraversalRuntimeContext {

    private DagTopologyGroup topology;

    private RowVertex vertex;

    private long inputOpId;

    private long currentOpId;

    private MessageBox messageBox;

    private ParameterRequest request;

    protected TraversalVertexQuery<Object, Row> vertexQuery;

    protected TraversalEdgeQuery<Object, Row> edgeQuery;

    // opId -> CallContext stack.
    private final Map<Long, Stack<CallContext>> callStacks = new HashMap<>();

    private final Set<CallRequestId> callRequestIds = new HashSet<>();

    private final Map<Object, Object[]> vertexId2AppendFields = new HashMap<>();

    protected VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext;

    public AbstractTraversalRuntimeContext(TraversalVertexQuery<Object, Row> vertexQuery,
                                           TraversalEdgeQuery<Object, Row> edgeQuery) {
        this.vertexQuery = vertexQuery;
        this.edgeQuery = edgeQuery;
    }

    public AbstractTraversalRuntimeContext() {

    }

    @Override
    public DagTopologyGroup getTopology() {
        return topology;
    }

    @Override
    public void setTopology(DagTopologyGroup topology) {
        this.topology = topology;
    }

    @Override
    public RowVertex getVertex() {
        return vertex;
    }

    @Override
    public void setVertex(RowVertex vertex) {
        this.vertex = vertex;
        vertexQuery.withId(vertex.getId());
        edgeQuery.withId(vertex.getId());
    }

    @Override
    public Object getRequestId() {
        if (request != null) {
            return request.getRequestId();
        }
        return null;
    }

    @Override
    public void setCurrentOpId(long operatorId) {
        this.currentOpId = operatorId;
    }

    @Override
    public long getCurrentOpId() {
        return currentOpId;
    }

    @Override
    public void setRequest(ParameterRequest parameterRequest) {
        this.request = parameterRequest;
    }

    @Override
    public ParameterRequest getRequest() {
        return request;
    }

    @Override
    public void setMessageBox(MessageBox messageBox) {
        this.messageBox = messageBox;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends IMessage> M getMessage(MessageType messageType) {
        if (messageBox != null) {
            M message = messageBox.getMessage(currentOpId, messageType);
            if (message instanceof RequestIsolationMessage && getRequestId() != null) {
                message = (M) ((RequestIsolationMessage) message).getMessageByRequestId(getRequestId());
            }

            if (messageType == MessageType.PATH) {
                // Fetch path from the calling stack
                ITreePath pathMessage = (ITreePath) message;
                if (callStacks.containsKey(currentOpId)) {
                    Stack<CallContext> callStack = callStacks.get(currentOpId);

                    if (!callStack.isEmpty()) {
                        CallContext callContext = callStack.peek();
                        ITreePath stashTreePath = callContext.getPath(getRequestId(), getVertex().getId());
                        if (pathMessage != null) {
                            return (M) stashTreePath.merge(pathMessage);
                        }
                        return (M) stashTreePath;
                    }
                }
            } else if (messageType == MessageType.PARAMETER_REQUEST) {
                // Fetch request from the calling stack
                ParameterRequestMessage requestMessage = (ParameterRequestMessage) message;
                if (requestMessage == null) {
                    requestMessage = new ParameterRequestMessage();
                }
                Stack<CallContext> callStack = callStacks.get(currentOpId);
                if (callStack != null && !callStack.isEmpty()) {
                    CallContext callContext = callStack.peek();
                    List<ParameterRequest> stashRequests = callContext.getRequests(getVertex().getId());
                    if (stashRequests != null) {
                        for (ParameterRequest request : stashRequests) {
                            requestMessage.addRequest(request);
                        }
                    }
                    return (M) requestMessage;
                }
            }
            return message;
        }
        return null;
    }

    @Override
    public EdgeGroup loadEdges(IFilter loadEdgesFilter) {
        return EdgeGroup.of((CloseableIterator) edgeQuery.getEdges(loadEdgesFilter));
    }

    @Override
    public RowVertex loadVertex(Object id, IFilter loadVertexFilter, GraphSchema graphSchema,
                                IType<?>[] addingVertexFieldTypes) {
        RowVertex vertexFromState = (RowVertex) vertexQuery.withId(id).get(loadVertexFilter);
        if (addingVertexFieldTypes.length > 0) {
            Object[] appendFields = vertexId2AppendFields.get(vertexFromState.getId());
            if (appendFields == null) {
                appendFields = new Object[addingVertexFieldTypes.length];
            }
            VertexType stateVertexSchema = graphSchema.getVertex(vertexFromState.getLabel());
            return appendFields(vertexFromState, stateVertexSchema, appendFields);
        }
        return vertexFromState;
    }

    @Override
    public CloseableIterator<Object> loadAllVertex() {
        return vertexQuery.loadIdIterator();
    }

    private RowVertex appendFields(RowVertex vertexFromState, VertexType stateVertexSchema,
                                   Object[] appendFields) {
        Row value = vertexFromState.getValue();
        Object[] fields = value.getFields(stateVertexSchema.getValueTypes());
        Object[] newFields = new Object[fields.length + appendFields.length];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        System.arraycopy(appendFields, 0, newFields, fields.length, appendFields.length);
        Row newValue = ObjectRow.create(newFields);
        return (RowVertex) vertexFromState.withValue(newValue);
    }

    @Override
    public void push(long opId, CallContext callContext) {
        callStacks.computeIfAbsent(opId, k -> new Stack<>()).push(callContext);
    }

    @Override
    public void pop(long opId) {
        if (callStacks.containsKey(opId)) {
            Stack<CallContext> callStack = callStacks.get(opId);
            callStack.pop();
            if (callStack.isEmpty()) {
                callStacks.remove(opId);
            }
        }
    }

    @Override
    public void sendMessage(Object vertexId, IMessage message, long receiverId, long... otherReceiveIds) {
        MessageBox messageBox = message.getType().createMessageBox();
        messageBox.addMessage(receiverId, message);

        for (long otherReceiverId : otherReceiveIds) {
            messageBox.addMessage(otherReceiverId, message);
        }
        sendMessage(vertexId, messageBox);
    }

    protected abstract void sendMessage(Object vertexId, MessageBox messageBox);

    @SuppressWarnings("unchecked")
    @Override
    public void broadcast(IMessage message, long receiverId, long... otherReceiveIds) {
        BroadcastId id = new BroadcastId(getTaskIndex());
        MessageBox messageBox = message.getType().createMessageBox();
        messageBox.addMessage(receiverId, message);

        for (long otherReceiverId : otherReceiveIds) {
            messageBox.addMessage(otherReceiverId, message);
        }
        sendBroadcastMessage(id, messageBox);
    }

    protected abstract void sendBroadcastMessage(Object vertexId, MessageBox messageBox);

    @Override
    public void stashCallRequestId(CallRequestId callRequestId) {
        callRequestIds.add(callRequestId);
    }

    @Override
    public Iterable<CallRequestId> takeCallRequestIds() {
        Set<CallRequestId> requestIds = new HashSet<>(callRequestIds);
        callRequestIds.clear();
        return requestIds;
    }

    @Override
    public void setInputOperatorId(long id) {
        this.inputOpId = id;
    }

    @Override
    public long getInputOperatorId() {
        return this.inputOpId;
    }

    @Override
    public void addFieldToVertex(Object vertexId, int updateIndex, Object value) {
        //Here append value to the existed store
        Object[] appendFields;
        Object[] existFields = vertexId2AppendFields.get(vertexId);
        if (existFields == null) {
            appendFields = new Object[updateIndex + 1];
        } else if (updateIndex >= existFields.length) {
            appendFields = new Object[updateIndex + 1];
            System.arraycopy(existFields, 0, appendFields, 0, existFields.length);
        } else {
            appendFields = existFields;
        }
        appendFields[updateIndex] = value;
        vertexId2AppendFields.put(vertexId, appendFields);
    }

    public int getNumTasks() {
        return getRuntimeContext().getTaskArgs().getParallelism();
    }

    @Override
    public int getTaskIndex() {
        return getRuntimeContext().getTaskArgs().getTaskIndex();
    }

    @Override
    public long createUniqueId(long idInTask) {
        return IDUtil.uniqueId(getNumTasks(), getTaskIndex(), idInTask);
    }

    @Override
    public MetricGroup getMetric() {
        return getRuntimeContext().getMetric();
    }

    @Override
    public VertexCentricAggContext<ITraversalAgg, ITraversalAgg> getAggContext() {
        return aggContext;
    }

    @Override
    public void setAggContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.aggContext = Objects.requireNonNull(aggContext);
    }
}
