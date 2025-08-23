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

package org.apache.geaflow.state.context;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.descriptor.BaseStateDescriptor;
import org.apache.geaflow.state.graph.StateMode;
import org.apache.geaflow.utils.keygroup.KeyGroup;

/**
 * This class describe the external dependencies of state sub system.
 */
public class StateContext {

    private BaseStateDescriptor descriptor;
    private Configuration config;
    private int shardId;
    private boolean isLocalStore;

    public StateContext(BaseStateDescriptor descriptor, Configuration config) {
        this.descriptor = descriptor;
        this.config = config;
    }

    public StateContext withShardId(int shardId) {
        this.shardId = shardId;
        return this;
    }

    public StateContext withLocalStore(boolean localStore) {
        this.isLocalStore = localStore;
        return this;
    }

    public String getName() {
        return descriptor.getName();
    }

    public Configuration getConfig() {
        return config;
    }

    public MetricGroup getMetricGroup() {
        return descriptor.getMetricGroup();
    }

    public KeyGroup getKeyGroup() {
        return descriptor.getKeyGroup();
    }

    public String getStoreType() {
        return descriptor.getStoreType();
    }

    public BaseStateDescriptor getDescriptor() {
        return descriptor;
    }

    public int getShardId() {
        return shardId;
    }

    public boolean isLocalStore() {
        return isLocalStore;
    }

    public int getTotalShardNum() {
        return getDescriptor().getAssigner().getKeyGroupNumber();
    }

    public DataModel getDataModel() {
        return this.descriptor.getDateModel();
    }

    public StateMode getStateMode() {
        return this.descriptor.getStateMode();
    }

    public StateContext clone() {
        return new StateContext(descriptor, config)
            .withShardId(shardId)
            .withLocalStore(isLocalStore);
    }
}
