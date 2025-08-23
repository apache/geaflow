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

package org.apache.geaflow.dsl.runtime.traversal;

import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction.VertexCentricAggContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.runtime.expression.subquery.CallContext;
import org.apache.geaflow.dsl.runtime.traversal.data.CallRequestId;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.message.IMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageBox;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.state.pushdown.filter.IFilter;

public interface TraversalRuntimeContext {

    Configuration getConfig();

    DagTopologyGroup getTopology();

    void setTopology(DagTopologyGroup topology);

    long getIterationId();

    RowVertex getVertex();

    void setVertex(RowVertex vertex);

    Object getRequestId();

    void setCurrentOpId(long operatorId);

    long getCurrentOpId();

    void setRequest(ParameterRequest parameterRequest);

    ParameterRequest getRequest();

    default Row getParameters() {
        if (getRequest() != null) {
            return getRequest().getParameters();
        }
        return null;
    }

    void setMessageBox(MessageBox messageBox);

    <M extends IMessage> M getMessage(MessageType messageType);

    EdgeGroup loadEdges(IFilter loadEdgesFilter);

    RowVertex loadVertex(Object vertexId, IFilter loadVertexFilter, GraphSchema graphSchema, IType<?>[] addingVertexFieldTypes);

    CloseableIterator<Object> loadAllVertex();

    void sendMessage(Object vertexId, IMessage message, long receiverId, long... otherReceiveIds);

    void broadcast(IMessage message, long receiverId, long... otherReceiveIds);

    void takePath(ITreePath treePath);

    void sendCoordinator(String name, Object value);

    VertexCentricAggContext<ITraversalAgg, ITraversalAgg> getAggContext();

    void setAggContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext);

    RuntimeContext getRuntimeContext();

    MetricGroup getMetric();

    int getNumTasks();

    int getTaskIndex();

    void push(long opId, CallContext callContext);

    void pop(long opId);

    void stashCallRequestId(CallRequestId requestId);

    Iterable<CallRequestId> takeCallRequestIds();

    void setInputOperatorId(long id);

    long getInputOperatorId();

    void addFieldToVertex(Object vertexId, int index, Object value);

    long createUniqueId(long idInTask);
}
