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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.geaflow.state.action.ActionRequest;
import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.action.EmptyAction;
import org.apache.geaflow.state.action.IAction;
import org.apache.geaflow.state.action.StateActionContext;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.store.IStatefulStore;

public abstract class BaseActionAccess {

    private Lock lock = new ReentrantLock();
    protected Map<ActionType, IAction> registeredAction = new HashMap<>();

    protected abstract List<ActionType> allowActionTypes();

    protected void initAction(IStatefulStore baseStore, StateContext stateContext) {
        List<ActionType> allowActionTypes = allowActionTypes();
        for (ActionType actionType : allowActionTypes()) {
            if (allowActionTypes.contains(actionType)) {
                IAction action = ActionBuilder.build(actionType);
                action.init(new StateActionContext(baseStore, stateContext.getConfig()));
                this.registerAction(action);
            } else {
                this.registerAction(new EmptyAction(actionType));
            }
        }
    }

    public void registerAction(IAction action) {
        if (action != null) {
            this.registeredAction.put(action.getActionType(), action);
        }
    }

    public void doStoreAction(int shard, ActionType actionType, ActionRequest request) {
        request.setShard(shard);
        lock.lock();
        this.registeredAction.get(actionType).apply(request);
        lock.unlock();
    }
}
