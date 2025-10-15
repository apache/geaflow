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

package org.apache.geaflow.memory.array;

import java.nio.ByteBuffer;
import org.apache.geaflow.memory.MemoryViewReader;
import org.apache.geaflow.memory.MemoryViewReference;

public class ByteArrayWithMemoryView implements ByteArray {

    private int offset;
    private int len;
    private MemoryViewReference viewRef;

    public ByteArrayWithMemoryView(MemoryViewReference viewRef, int offset, int len) {
        this.offset = offset;
        this.len = len;
        this.viewRef = viewRef;
        this.viewRef.incRef();
        if (ByteArrayRefUtil.needCheck) {
            ByteArrayRefUtil.add(this);
        }
    }

    @Override
    public int size() {
        return this.len;
    }

    @Override
    public byte[] array() {
        byte[] bytes = new byte[len];
        getReader().read(bytes, 0, len);
        return bytes;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(array(), 0, len);
    }

    private MemoryViewReader getReader() {
        MemoryViewReader reader = viewRef.getMemoryView().getReader();
        reader.skip(offset);
        return reader;
    }

    @Override
    public boolean release() {
        if (len < 0) {
            return true;
        }

        synchronized (viewRef) {
            if (len > 0) {
                if (ByteArrayRefUtil.needCheck) {
                    ByteArrayRefUtil.remove(this);
                }
                len = -1;
                return viewRef.decRef();
            }
        }
        return false;
    }
}
