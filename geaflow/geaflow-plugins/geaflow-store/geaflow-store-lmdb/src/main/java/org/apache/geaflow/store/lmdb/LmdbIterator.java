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

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LmdbIterator implements CloseableIterator<Tuple<byte[], byte[]>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LmdbIterator.class);

    private final CursorIterable<ByteBuffer> cursorIterable;
    private final Iterator<KeyVal<ByteBuffer>> iterator;
    private final Txn<ByteBuffer> txn;
    private final byte[] prefix;
    private Tuple<byte[], byte[]> nextValue;
    private boolean closed;

    public LmdbIterator(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn, byte[] prefix) {
        this.txn = txn;
        this.prefix = prefix;
        this.closed = false;

        if (prefix != null && prefix.length > 0) {
            ByteBuffer prefixBuffer = ByteBuffer.allocateDirect(prefix.length);
            prefixBuffer.put(prefix).flip();
            this.cursorIterable = dbi.iterate(txn, KeyRange.atLeast(prefixBuffer));
        } else {
            this.cursorIterable = dbi.iterate(txn);
        }
        this.iterator = cursorIterable.iterator();
        this.nextValue = null;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }

        // If we already have a next value cached, return true
        if (nextValue != null) {
            return true;
        }

        // Try to fetch the next valid value
        while (iterator.hasNext()) {
            KeyVal<ByteBuffer> kv = iterator.next();
            byte[] key = toByteArray(kv.key());
            byte[] value = toByteArray(kv.val());

            // Check prefix match if prefix is specified
            if (prefix != null && prefix.length > 0) {
                if (!startsWith(key, prefix)) {
                    // Reached end of prefix range
                    return false;
                }
            }

            nextValue = Tuple.of(key, value);
            return true;
        }

        return false;
    }

    @Override
    public Tuple<byte[], byte[]> next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more elements in iterator");
        }

        Tuple<byte[], byte[]> result = nextValue;
        nextValue = null;
        return result;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        if (cursorIterable != null) {
            try {
                cursorIterable.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing LMDB cursor", e);
            }
        }
        if (txn != null) {
            try {
                txn.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing LMDB transaction", e);
            }
        }
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind(); // Reset position for potential reuse
        return bytes;
    }

    private boolean startsWith(byte[] key, byte[] prefix) {
        if (key == null || prefix == null || key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
