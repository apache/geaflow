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

package org.apache.geaflow.operator.impl.graph.algo.vc.msgbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;

public class CombinedMsgBox<K, MESSAGE> implements IGraphMsgBox<K, MESSAGE> {

    private final Map<K, MESSAGE> inMessageBox;
    private final Map<K, MESSAGE> outMessageBox;

    private final VertexCentricCombineFunction<MESSAGE> combineFunction;

    public CombinedMsgBox(VertexCentricCombineFunction<MESSAGE> combineFunction) {
        this.combineFunction = combineFunction;
        this.inMessageBox = new HashMap<>();
        this.outMessageBox = new HashMap<>();
    }

    @Override
    public void addInMessages(K vertexId, MESSAGE message) {
        MESSAGE oldMessage = inMessageBox.get(vertexId);
        if (oldMessage != null) {
            MESSAGE newMessage = this.combineFunction.combine(oldMessage, message);
            inMessageBox.put(vertexId, newMessage);
        } else {
            this.inMessageBox.put(vertexId, message);
        }
    }

    @Override
    public void processInMessage(MsgProcessFunc<K, MESSAGE> processFunc) {
        processMessage(inMessageBox, processFunc);
    }

    @Override
    public void clearInBox() {
        this.inMessageBox.clear();
    }

    @Override
    public void addOutMessage(K vertexId, MESSAGE message) {
        addMessage(outMessageBox, vertexId, message);
    }

    @Override
    public void processOutMessage(MsgProcessFunc<K, MESSAGE> processFunc) {
        processMessage(outMessageBox, processFunc);
    }

    @Override
    public void clearOutBox() {
        this.outMessageBox.clear();
    }

    private void processMessage(Map<K, MESSAGE> messageBox, MsgProcessFunc<K, MESSAGE> processFunc) {
        for (Entry<K, MESSAGE> entry : messageBox.entrySet()) {
            processFunc.process(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
    }

    private void addMessage(Map<K, MESSAGE> messageBox, K vertexId, MESSAGE message) {
        MESSAGE oldMessage = messageBox.get(vertexId);
        if (oldMessage != null) {
            MESSAGE newMessage = this.combineFunction.combine(oldMessage, message);
            messageBox.put(vertexId, newMessage);
        } else {
            messageBox.put(vertexId, message);
        }
    }

    @VisibleForTesting
    protected Map<K, MESSAGE> getInMessageBox() {
        return this.inMessageBox;
    }

    @VisibleForTesting
    protected Map<K, MESSAGE> getOutMessageBox() {
        return this.outMessageBox;
    }

}
