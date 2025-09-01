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

package org.apache.geaflow.store.rocksdb.iterator;

import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.utils.ByteUtils;
import org.rocksdb.RocksIterator;

public class RocksdbIterator implements CloseableIterator<Tuple<byte[], byte[]>> {

    private final RocksIterator rocksIt;
    private byte[] prefix;
    private Tuple<byte[], byte[]> next;
    private boolean isClosed = false;

    public RocksdbIterator(RocksIterator iterator) {
        this.rocksIt = iterator;
        this.rocksIt.seekToFirst();
    }

    public RocksdbIterator(RocksIterator iterator, byte[] prefix) {
        this.rocksIt = iterator;
        this.prefix = prefix;
        this.rocksIt.seek(prefix);
    }

    private boolean isValid(byte[] key) {
        return prefix == null || ByteUtils.isStartsWith(key, prefix);
    }

    @Override
    public boolean hasNext() {
        next = null;
        if (!isClosed && this.rocksIt.isValid()) {
            next = Tuple.of(this.rocksIt.key(), this.rocksIt.value());
        }
        if (next == null || !isValid(next.f0)) {
            close();
            return false;
        }
        return true;
    }

    @Override
    public Tuple<byte[], byte[]> next() {
        this.rocksIt.next();
        return next;
    }

    @Override
    public void close() {
        if (!isClosed) {
            this.rocksIt.close();
            isClosed = true;
        }
    }
}
