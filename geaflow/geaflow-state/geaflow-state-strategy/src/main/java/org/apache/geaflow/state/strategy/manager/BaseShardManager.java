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
import java.util.Map.Entry;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.pushdown.KeyGroupStatePushDown;
import org.apache.geaflow.state.strategy.accessor.IAccessor;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseShardManager<K, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseShardManager.class);

    protected final int totalShardNum;
    protected Map<Integer, T> traitMap;
    protected KeyGroup shardGroup;
    protected IKeyGroupAssigner assigner;
    protected boolean mayScale;

    public BaseShardManager(StateContext context, Map<Integer, IAccessor> accessorMap) {
        this.shardGroup = context.getKeyGroup();
        this.assigner = context.getDescriptor().getAssigner();
        Preconditions.checkArgument(this.assigner != null, "The assigner must be not null");
        LOGGER.info("key group {}, key group num {}", this.shardGroup, this.assigner.getKeyGroupNumber());
        this.mayScale = context.isLocalStore();
        this.totalShardNum = this.assigner.getKeyGroupNumber();
        this.traitMap = new HashMap<>(accessorMap.size());
        for (Entry<Integer, IAccessor> entry : accessorMap.entrySet()) {
            this.traitMap.put(entry.getKey(), (T) entry.getValue());
        }
    }

    protected T getTraitByKey(K key) {
        return getTraitById(assigner.assign(key));
    }

    protected T getTraitById(int keyGroupId) {
        T trait = traitMap.get(keyGroupId);
        if (trait == null) {
            throw new GeaflowRuntimeException(
                "we have " + traitMap.keySet() + " need keyGroupId " + keyGroupId);

        }
        return trait;
    }

    protected KeyGroup getShardGroup(IStatePushDown pushdown) {
        KeyGroup queryShardGroup = this.shardGroup;
        if (pushdown instanceof KeyGroupStatePushDown) {
            queryShardGroup = ((KeyGroupStatePushDown) pushdown).getKeyGroup();
            Preconditions.checkArgument(this.shardGroup.contains(queryShardGroup),
                "state keyGroup %s, query keyGroup %s", this.shardGroup, queryShardGroup);
        }
        return queryShardGroup;
    }
}
