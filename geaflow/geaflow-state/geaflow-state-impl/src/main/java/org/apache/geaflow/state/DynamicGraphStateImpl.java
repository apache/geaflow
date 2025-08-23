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

package org.apache.geaflow.state;

import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class DynamicGraphStateImpl<K, VV, EV> implements DynamicGraphState<K, VV, EV> {

    private final IGraphManager<K, VV, EV> graphManager;
    private DynamicVertexState<K, VV, EV> vertexState;
    private DynamicEdgeState<K, VV, EV> edgeState;
    private DynamicOneDegreeGraphState<K, VV, EV> oneDegreeGraphState;

    public DynamicGraphStateImpl(IGraphManager<K, VV, EV> graphManager) {
        this.graphManager = graphManager;
    }

    @Override
    public DynamicVertexState<K, VV, EV> V() {
        if (vertexState == null) {
            vertexState = new DynamicVertexStateImpl<>(this.graphManager);
        }
        return vertexState;
    }

    @Override
    public DynamicEdgeState<K, VV, EV> E() {
        if (edgeState == null) {
            edgeState = new DynamicEdgeStateImpl<>(this.graphManager);
        }
        return edgeState;
    }

    @Override
    public DynamicOneDegreeGraphState<K, VV, EV> VE() {
        if (oneDegreeGraphState == null) {
            oneDegreeGraphState = new DynamicOneDegreeGraphStateImpl<>(this.graphManager);
        }
        return oneDegreeGraphState;
    }
}
