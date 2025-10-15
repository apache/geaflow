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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.geaflow.state.action.ActionType;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.descriptor.BaseKeyDescriptor;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.IStatefulStore;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.context.StoreContext;

public class RWKeyAccessor<K> extends BaseActionAccess implements IAccessor {

    protected IStatefulStore store;

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.store = (IStatefulStore) storeBuilder.getStore(context.getDataModel(),
            context.getConfig());

        BaseKeyDescriptor<K> desc = (BaseKeyDescriptor<K>) context.getDescriptor();

        StoreContext storeContext = new StoreContext(context.getName()).withConfig(
                context.getConfig()).withMetricGroup(context.getMetricGroup())
            .withShardId(context.getShardId()).withKeySerializer(desc.getKeySerializer());

        this.store.init(storeContext);
        initAction(this.store, context);
    }

    @Override
    public IBaseStore getStore() {
        return this.store;
    }

    protected List<ActionType> allowActionTypes() {
        return Stream.of(ActionType.values()).collect(Collectors.toList());
    }
}
