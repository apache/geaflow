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

package org.apache.geaflow.state.strategy.manager;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.state.action.ActionRequest;
import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.manage.LoadOption;
import org.apache.geaflow.state.strategy.accessor.AccessorBuilder;
import org.apache.geaflow.state.strategy.accessor.IAccessor;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public class BaseStateManager {

    protected StateContext context;
    protected Configuration config;
    protected MetricGroup metricGroup;
    protected KeyGroup keyGroup;
    protected Map<Integer, IAccessor> accessorMap = new HashMap<>();

    public void init(StateContext context) {
        this.context = context;
        this.config = context.getConfig();
        this.metricGroup = context.getMetricGroup();
        this.keyGroup = context.getKeyGroup();
        IStoreBuilder storeBuilder = StoreBuilderFactory.build(context.getStoreType());
        this.context.withLocalStore(storeBuilder.getStoreDesc().isLocalStore());

        for (int shardId = keyGroup.getStartKeyGroup(); shardId <= keyGroup.getEndKeyGroup();
             shardId++) {
            IAccessor accessor = AccessorBuilder.getAccessor(context.getDataModel(),
                context.getStateMode());
            StateContext newContext = context.clone().withShardId(shardId);
            accessor.init(newContext, storeBuilder);

            this.accessorMap.put(shardId, accessor);
        }
    }

    public void doStoreAction(ActionType actionType, ActionRequest request) {
        if (actionType == ActionType.LOAD) {
            KeyGroup loadKeyGroup = ((LoadOption) request.getRequest()).getKeyGroup();
            Preconditions.checkArgument(
                loadKeyGroup == null || this.keyGroup.contains(loadKeyGroup));
        }
        this.accessorMap.forEach((key, value) -> value.doStoreAction(key, actionType, request));

    }
}
