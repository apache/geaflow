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

import java.util.Objects;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageBox;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;

public class GeaFlowStaticVCTraversal extends VertexCentricTraversal<Object, Row, Row, MessageBox, ITreePath> {

    private final ExecuteDagGroup executeDagGroup;

    private final boolean isTraversalAllWithRequest;

    public GeaFlowStaticVCTraversal(ExecuteDagGroup executeDagGroup,
                                    int maxTraversal,
                                    boolean isTraversalAllWithRequest) {
        super(maxTraversal);
        this.executeDagGroup = Objects.requireNonNull(executeDagGroup);
        this.isTraversalAllWithRequest = isTraversalAllWithRequest;
    }

    @Override
    public VertexCentricCombineFunction<MessageBox> getCombineFunction() {
        return new MessageBoxCombineFunction();
    }

    @Override
    public IEncoder<MessageBox> getMessageEncoder() {
        return null;
    }

    @Override
    public VertexCentricTraversalFunction<Object, Row, Row, MessageBox, ITreePath> getTraversalFunction() {
        return new GeaFlowStaticVCTraversalFunction(executeDagGroup, isTraversalAllWithRequest);
    }

    private static class MessageBoxCombineFunction implements VertexCentricCombineFunction<MessageBox> {

        @Override
        public MessageBox combine(MessageBox oldMessage, MessageBox newMessage) {
            return newMessage.combine(oldMessage);
        }
    }
}
