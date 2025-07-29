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

package org.apache.geaflow.operator.impl.window.incremental;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import java.util.HashSet;
import java.util.Set;
import org.apache.geaflow.api.function.base.AggregateFunction;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.trait.CheckpointTrait;
import org.apache.geaflow.api.trait.TransactionTrait;
import org.apache.geaflow.operator.base.window.AbstractOneInputOperator;
import org.apache.geaflow.state.KeyValueState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;

public class IncrAggregateOperator<KEY, IN, ACC, OUT> extends
    AbstractOneInputOperator<IN, AggregateFunction<IN, ACC, OUT>>
    implements TransactionTrait, CheckpointTrait {

    private transient KeyValueState<KEY, ACC> aggregatingState;
    private KeySelector<IN, KEY> keySelector;
    private Set<KEY> keySet;

    public IncrAggregateOperator(AggregateFunction<IN, ACC, OUT> aggregateFunction,
                                 KeySelector<IN, KEY> keySelector) {
        super(aggregateFunction);
        this.keySelector = keySelector;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        KeyValueStateDescriptor descriptor = KeyValueStateDescriptor.build(
            getIdentify(),
            this.runtimeContext.getConfiguration().getString(SYSTEM_STATE_BACKEND_TYPE));
        int taskIndex = this.runtimeContext.getTaskArgs().getTaskIndex();
        int parallelism = this.runtimeContext.getTaskArgs().getParallelism();
        int maxParallelism = this.runtimeContext.getTaskArgs().getMaxParallelism();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxParallelism);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);
        this.aggregatingState = StateFactory.buildKeyValueState(descriptor, this.runtimeContext.getConfiguration());
        this.keySet = new HashSet<>();
    }

    @Override
    protected void process(IN value) throws Exception {
        KEY key = this.keySelector.getKey(value);
        ACC acc = this.aggregatingState.get(key);

        if (acc == null) {
            acc = this.function.createAccumulator();
        }
        keySet.add(key);
        this.function.add(value, acc);
        aggregatingState.put(key, acc);
    }

    @Override
    public void finish(long windowId) {
        for (KEY key : keySet) {
            ACC acc = aggregatingState.get(key);
            OUT result = this.function.getResult(acc);
            if (result != null) {
                collectValue(result);
            }
        }
        keySet.clear();
    }

    @Override
    public void rollback(long windowId) {
        this.aggregatingState.manage().operate().setCheckpointId(windowId);
        this.aggregatingState.manage().operate().recover();
    }

    @Override
    public void checkpoint(long windowId) {
        this.aggregatingState.manage().operate().setCheckpointId(windowId);
        this.aggregatingState.manage().operate().finish();
        this.aggregatingState.manage().operate().archive();
    }
}
