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

package org.apache.geaflow.dsl.runtime.expression.subquery;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.expression.AbstractExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.CallRequestId;
import org.apache.geaflow.dsl.runtime.traversal.data.EndOfData;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.SingleValue;
import org.apache.geaflow.dsl.runtime.traversal.message.EODMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;
import org.apache.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.ReturnMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl.ReturnKey;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.TreePaths;

public class CallQueryExpression extends AbstractExpression implements ICallQuery {

    private final String queryName;

    /**
     * The operator id of the start operator for the sub query dag.
     */
    private final long queryId;

    private final int startVertexIndex;

    private final VertexType startVertexType;

    private final IType<?> returnType;

    private TraversalRuntimeContext context;

    private CallState callState;

    private final int[] refParentPathIndices;

    private final Object defaultAggValue;

    public CallQueryExpression(String queryName, long queryId,
                               int startVertexIndex, VertexType startVertexType,
                               IType<?> returnType, int[] refParentPathIndices,
                               Object defaultAggValue) {
        this.queryName = queryName;
        this.queryId = queryId;
        this.startVertexIndex = startVertexIndex;
        this.startVertexType = startVertexType;
        this.returnType = returnType;
        this.refParentPathIndices = refParentPathIndices;
        this.defaultAggValue = defaultAggValue;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        this.context = context;
        this.open(FunctionContext.of(context.getConfig()));
        this.callState = CallState.INIT;
    }

    @Override
    public void setCallState(CallState callState) {
        this.callState = callState;
    }

    @Override
    public CallState getCallState() {
        return callState;
    }

    @Override
    public Object evaluate(Row row) {
        RowVertex startVertex = (RowVertex) row.getField(startVertexIndex, startVertexType);
        Path path = (Path) row;
        if (callState == CallState.WAITING) {
            ParameterRequestMessage requestMessage = new ParameterRequestMessage();
            Row parameter = context.getParameters();
            long uniquePathId = context.createUniqueId(path.getId());
            CallRequestId callRequestId = new CallRequestId(uniquePathId, context.getCurrentOpId(),
                startVertex.getId());
            ParameterRequest request = new ParameterRequest(callRequestId, startVertex.getId(), parameter);

            requestMessage.addRequest(request);
            // send request message to sub query plan's start query operator id.
            context.sendMessage(startVertex.getId(), requestMessage, queryId);
            Path pathMessage = ((Path) row).subPath(refParentPathIndices);
            ITreePath treePath = TreePaths.createTreePath(Collections.singletonList(pathMessage));
            treePath.setRequestIdForTree(callRequestId);
            context.sendMessage(startVertex.getId(), treePath, queryId);
            return null;
        } else if (callState == CallState.RETURNING) {
            ReturnMessage returnMessage = context.getMessage(MessageType.RETURN_VALUE);
            long uniquePathId = context.createUniqueId(path.getId());
            ReturnKey returnKey = new ReturnKey(uniquePathId, queryId);
            SingleValue singleValue = returnMessage.getValue(returnKey);
            if (singleValue == null) {
                return defaultAggValue;
            }
            return singleValue.getValue(returnType);
        }
        throw new IllegalArgumentException("Illegal call state: " + callState + " for evaluate() method");
    }

    public void sendEod() {
        EndOfData eod = EndOfData.of(context.getCurrentOpId(), context.getCurrentOpId());
        context.broadcast(EODMessage.of(eod), queryId);
    }

    @Override
    public void finishCall() {

    }

    public String getQueryName() {
        return queryName;
    }

    @Override
    public String showExpression() {
        return "Call(" + queryName + ")";
    }

    @Override
    public IType<?> getOutputType() {
        return returnType;
    }

    @Override
    public List<Expression> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public List<Integer> getRefPathFieldIndices() {
        return ArrayUtil.toList(refParentPathIndices);
    }

    public enum CallState {
        INIT,
        CALLING,
        WAITING,
        RETURNING,
        FINISH
    }
}
