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

package org.apache.geaflow.state.strategy.accessor;

import org.apache.geaflow.state.action.ActionRequest;
import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.action.IAction;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.IStoreBuilder;

public interface IAccessor {

    void init(StateContext stateContext, IStoreBuilder storeBuilder);

    IBaseStore getStore();

    // action
    void registerAction(IAction action);

    void doStoreAction(int shardId, ActionType actionType, ActionRequest request);
}
