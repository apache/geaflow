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

public class StaticGraphStateImpl<K, VV, EV> implements StaticGraphState<K, VV, EV> {

    private final IGraphManager<K, VV, EV> graphManager;
    private StaticVertexState<K, VV, EV> vertexState;
    private StaticEdgeState<K, VV, EV> edgeState;
    private StaticOneDegreeGraphState<K, VV, EV> oneDegreeGraphState;

    public StaticGraphStateImpl(IGraphManager<K, VV, EV> graphManager) {
        this.graphManager = graphManager;
        this.vertexState = new StaticVertexStateImpl<>(this.graphManager);
        this.edgeState = new StaticEdgeStateImpl<>(this.graphManager);
        this.oneDegreeGraphState = new StaticOneDegreeGraphStateImpl<>(this.graphManager);
    }

    @Override
    public StaticVertexState<K, VV, EV> V() {
        return vertexState;
    }

    @Override
    public StaticEdgeState<K, VV, EV> E() {
        return edgeState;
    }

    @Override
    public StaticOneDegreeGraphState<K, VV, EV> VE() {
        return oneDegreeGraphState;
    }
}
