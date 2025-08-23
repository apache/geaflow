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
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.graph.DynamicGraphTrait;
import org.apache.geaflow.state.graph.StaticGraphTrait;
import org.apache.geaflow.state.pushdown.inner.IFilterConverter;
import org.apache.geaflow.state.strategy.accessor.IAccessor;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.api.graph.IPushDownStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphManagerImpl<K, VV, EV> extends BaseStateManager implements IGraphManager<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphManagerImpl.class);

    private StaticGraphTrait<K, VV, EV> staticGraphTrait;
    private DynamicGraphTrait<K, VV, EV> dynamicGraphTrait;
    private IFilterConverter filterConverter;

    public GraphManagerImpl() {

    }

    @Override
    public void init(StateContext context) {
        super.init(context);
    }

    @Override
    public StaticGraphTrait<K, VV, EV> getStaticGraphTrait() {
        if (staticGraphTrait == null) {
            staticGraphTrait = new StaticGraphManagerImpl<>(this.context, this.accessorMap);
        }
        return staticGraphTrait;
    }

    @Override
    public DynamicGraphTrait<K, VV, EV> getDynamicGraphTrait() {
        if (dynamicGraphTrait == null) {
            dynamicGraphTrait = new DynamicGraphManagerImpl<>(this.context, this.accessorMap);
        }
        return dynamicGraphTrait;
    }

    @Override
    public IFilterConverter getFilterConverter() {
        if (filterConverter == null) {
            if (this.accessorMap.values().size() > 0) {
                for (IAccessor value : this.accessorMap.values()) {
                    IBaseStore store = value.getStore();
                    if (store == null) {
                        continue;
                    }
                    if (store instanceof IPushDownStore) {
                        filterConverter = ((IPushDownStore) store).getFilterConverter();
                    }
                }
            }
        }
        return Preconditions.checkNotNull(filterConverter);
    }
}
