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

package org.apache.geaflow.state.descriptor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.graph.StateMode;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public abstract class BaseStateDescriptor implements Serializable {

    protected String name;
    protected String storeType;
    protected KeyGroup keyGroup;
    protected MetricGroup metricGroup;
    protected IKeyGroupAssigner assigner;
    protected StateMode stateMode = StateMode.RW;
    protected DataModel dateModel;

    protected BaseStateDescriptor(String name, String storeType) {
        this.name = name;
        this.storeType = storeType;
    }

    public BaseStateDescriptor withName(String name) {
        this.name = name;
        return this;
    }

    public BaseStateDescriptor withMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public BaseStateDescriptor withKeyGroup(KeyGroup keyGroup) {
        this.keyGroup = keyGroup;
        return this;
    }

    public BaseStateDescriptor withDataModel(DataModel dataModel) {
        this.dateModel = dataModel;
        return this;
    }

    public BaseStateDescriptor withStateMode(StateMode stateMode) {
        this.stateMode = stateMode;
        return this;
    }

    public BaseStateDescriptor withKeyGroupAssigner(IKeyGroupAssigner assigner) {
        this.assigner = assigner;
        return this;
    }

    public StateMode getStateMode() {
        return stateMode;
    }

    public DataModel getDateModel() {
        return dateModel;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public KeyGroup getKeyGroup() {
        return checkNotNull(keyGroup, "keyGroup must be set");
    }

    public String getName() {
        return checkNotNull(name, "descriptor name must be set");
    }

    public String getStoreType() {
        return checkNotNull(storeType, "storeType must be set");
    }

    public IKeyGroupAssigner getAssigner() {
        return assigner;
    }

    public abstract DescriptorType getDescriptorType();
}
