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

package org.apache.geaflow.state.manage;

import org.apache.geaflow.state.action.ActionRequest;
import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.strategy.manager.IStateManager;

public class StateOperatorImpl implements StateOperator {

    private final IStateManager stateManager;
    private long checkpointId;

    public StateOperatorImpl(IStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public void load(LoadOption loadOption) {
        if (loadOption.getCheckPointId() == 0) {
            loadOption.withCheckpointId(this.checkpointId);
        }
        this.stateManager.doStoreAction(ActionType.LOAD, new ActionRequest(loadOption));
    }

    @Override
    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public void finish() {
        this.stateManager.doStoreAction(ActionType.FINISH, new ActionRequest());
    }

    @Override
    public void compact() {
        this.stateManager.doStoreAction(ActionType.COMPACT, new ActionRequest());
    }

    @Override
    public void archive() {
        this.stateManager.doStoreAction(ActionType.ARCHIVE, new ActionRequest<>(checkpointId));
    }

    @Override
    public void recover() {
        this.stateManager.doStoreAction(ActionType.RECOVER, new ActionRequest<>(checkpointId));
    }

    @Override
    public void close() {
        this.stateManager.doStoreAction(ActionType.CLOSE, new ActionRequest());
    }

    @Override
    public void drop() {
        this.stateManager.doStoreAction(ActionType.DROP, new ActionRequest());
    }
}
