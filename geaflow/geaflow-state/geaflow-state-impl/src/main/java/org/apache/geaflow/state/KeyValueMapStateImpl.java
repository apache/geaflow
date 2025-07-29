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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.key.KeyValueTrait;

public class KeyValueMapStateImpl<K, UK, UV> extends BaseKeyStateImpl<K> implements KeyMapState<K, UK, UV> {

    private final KeyValueTrait<K, Map> kvTrait;

    public KeyValueMapStateImpl(StateContext context) {
        super(context);
        this.kvTrait = this.keyStateManager.getKeyValueTrait(Map.class);
    }

    @Override
    public Map<UK, UV> get(K key) {
        Map<UK, UV> map = this.kvTrait.get(key);
        return map == null ? new HashMap<>() : map;
    }

    @Override
    public List<UV> get(K key, UK... subKeys) {
        Map<UK, UV> map = get(key);
        return Arrays.stream(subKeys).map(map::get).collect(Collectors.toList());
    }

    @Override
    public void add(K key, UK subKey, UV value) {
        Map<UK, UV> map = get(key);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(subKey, value);
        add(key, map);
    }

    @Override
    public void add(K key, Map<UK, UV> map) {
        Map<UK, UV> tmp = get(key);
        if (tmp == null) {
            tmp = new HashMap<>();
        }
        tmp.putAll(map);
        put(key, tmp);
    }

    @Override
    public void remove(K key) {
        this.kvTrait.remove(key);
    }

    @Override
    public void put(K key, Map<UK, UV> map) {
        this.kvTrait.put(key, map);
    }

    @Override
    public void remove(K key, UK... subKeys) {
        Map<UK, UV> map = get(key);
        Arrays.stream(subKeys).forEach(map::remove);
        add(key, map);
    }
}
