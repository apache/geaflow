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

package org.apache.geaflow.store.lmdb;

import static org.apache.geaflow.store.lmdb.LmdbConfigKeys.DEFAULT_DB;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.api.key.IKVStatefulStore;
import org.apache.geaflow.store.context.StoreContext;

public class KVLmdbStore<K, V> extends BaseLmdbStore implements IKVStatefulStore<K, V> {

    private IKVSerializer<K, V> kvSerializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.kvSerializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(
            storeContext.getKeySerializer(), "keySerializer must be set");
    }

    @Override
    protected List<String> getDbList() {
        return Arrays.asList(DEFAULT_DB);
    }

    @Override
    public void put(K key, V value) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] valueArray = this.kvSerializer.serializeValue(value);
        this.lmdbClient.write(DEFAULT_DB, keyArray, valueArray);
    }

    @Override
    public void remove(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        this.lmdbClient.delete(DEFAULT_DB, keyArray);
    }

    @Override
    public V get(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] valueArray = this.lmdbClient.get(DEFAULT_DB, keyArray);
        if (valueArray == null) {
            return null;
        }
        return this.kvSerializer.deserializeValue(valueArray);
    }

    /**
     * Get an iterator over all key-value pairs in the store.
     *
     * @return iterator over key-value tuples
     */
    public CloseableIterator<Tuple<K, V>> getKeyValueIterator() {
        LmdbIterator rawIterator = this.lmdbClient.getIterator(DEFAULT_DB);
        return new CloseableIterator<Tuple<K, V>>() {
            @Override
            public boolean hasNext() {
                return rawIterator.hasNext();
            }

            @Override
            public Tuple<K, V> next() {
                Tuple<byte[], byte[]> rawTuple = rawIterator.next();
                K key = kvSerializer.deserializeKey(rawTuple.f0);
                V value = kvSerializer.deserializeValue(rawTuple.f1);
                return Tuple.of(key, value);
            }

            @Override
            public void close() {
                rawIterator.close();
            }
        };
    }
}
