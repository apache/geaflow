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

import java.util.List;
import java.util.Map;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.key.KeyListTrait;
import org.apache.geaflow.state.strategy.accessor.IAccessor;

public class KeyListManagerImpl<K, V> extends BaseShardManager<K, KeyListTrait<K, V>> implements KeyListTrait<K, V> {

    public KeyListManagerImpl(StateContext context, Map<Integer, IAccessor> accessorMap) {
        super(context, accessorMap);
    }

    @Override
    public List<V> get(K key) {
        return getTraitByKey(key).get(key);
    }

    @Override
    public void add(K key, V... value) {
        getTraitByKey(key).add(key, value);
    }

    @Override
    public void remove(K key) {
        getTraitByKey(key).remove(key);
    }
}
