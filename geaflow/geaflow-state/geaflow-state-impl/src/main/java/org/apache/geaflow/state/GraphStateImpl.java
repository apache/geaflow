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

import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.manage.ManageableGraphState;
import org.apache.geaflow.state.manage.ManageableGraphStateImpl;
import org.apache.geaflow.state.strategy.manager.GraphManagerImpl;
import org.apache.geaflow.state.strategy.manager.IGraphManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphStateImpl<K, VV, EV> implements GraphState<K, VV, EV> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphStateImpl.class);
    private final IGraphManager<K, VV, EV> graphManager;
    private final ManageableGraphState manageableGraphState;

    private StaticGraphState<K, VV, EV> staticGraphState;
    private DynamicGraphState<K, VV, EV> dynamicGraphState;

    public GraphStateImpl(StateContext context) {
        this.graphManager = new GraphManagerImpl<>();
        LOGGER.info("ThreadId {}, GraphStateImpl initDB", Thread.currentThread().getId());
        this.graphManager.init(context);
        this.manageableGraphState = new ManageableGraphStateImpl(this.graphManager);
        this.staticGraphState = new StaticGraphStateImpl<>(this.graphManager);
        this.dynamicGraphState = new DynamicGraphStateImpl<>(this.graphManager);
    }

    @Override
    public StaticGraphState<K, VV, EV> staticGraph() {
        return staticGraphState;
    }

    @Override
    public DynamicGraphState<K, VV, EV> dynamicGraph() {
        return dynamicGraphState;
    }

    @Override
    public ManageableGraphState manage() {
        return manageableGraphState;
    }

}
