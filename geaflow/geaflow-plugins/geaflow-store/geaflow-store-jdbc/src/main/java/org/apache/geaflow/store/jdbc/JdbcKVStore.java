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

package org.apache.geaflow.store.jdbc;

import com.google.common.base.Preconditions;
import java.sql.SQLException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcKVStore<K, V> extends BaseJdbcStore implements IKVStore<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcKVStore.class);
    private String[] columns = {"value"};

    private IKVSerializer<K, V> serializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.serializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(storeContext.getKeySerializer(),
            "keySerializer must be set");
    }

    private java.lang.String getFromKey(K key) {
        if (key.getClass() == byte[].class) {
            return new String((byte[]) key);
        }
        return key.toString();
    }

    @Override
    public V get(K key) {
        try {
            byte[] res = query(getFromKey(key), columns);
            if (res != null) {
                return serializer.deserializeValue(res);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("get fail", e);
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] valueArray = serializer.serializeValue(value);
        RetryCommand.run(() -> {
            try {
                String fromKey = getFromKey(key);
                if (!update(fromKey, columns, new Object[]{valueArray})) {
                    LOGGER.info("key: {}, insert fail, try insert", key);
                    try {
                        insert(fromKey, columns, new Object[]{valueArray});
                    } catch (Exception e) {
                        LOGGER.info("key: {}, insert fail", key);
                        throw new GeaflowRuntimeException("put fail");
                    }
                }
            } catch (SQLException e) {
                throw new GeaflowRuntimeException("put fail", e);
            }
            return true;
        }, retries);
    }

    @Override
    public void remove(K key) {
        try {
            delete(getFromKey(key));
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("remove fail", e);
        }
    }
}
