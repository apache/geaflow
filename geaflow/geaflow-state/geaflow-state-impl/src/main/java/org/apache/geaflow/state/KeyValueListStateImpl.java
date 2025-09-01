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

package org.apache.geaflow.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.key.KeyValueTrait;

public class KeyValueListStateImpl<K, V> extends BaseKeyStateImpl<K> implements KeyListState<K, V> {

    private final KeyValueTrait<K, List> kListTrait;

    public KeyValueListStateImpl(StateContext context) {
        super(context);
        this.kListTrait = this.keyStateManager.getKeyValueTrait(List.class);
    }

    @Override
    public List<V> get(K key) {
        List<V> list = this.kListTrait.get(key);
        return list == null ? new ArrayList<>() : list;
    }

    @Override
    public void add(K key, V... value) {
        List<V> list = get(key);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.addAll(Arrays.asList(value));
        put(key, list);
    }

    @Override
    public void remove(K key) {
        this.kListTrait.remove(key);
    }

    @Override
    public void put(K key, List<V> list) {
        this.kListTrait.put(key, list);
    }
}
