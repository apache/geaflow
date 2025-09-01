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

package org.apache.geaflow.memory.channel;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * This class is a writable channel that stores the written data in a byte array in memory.
 */
public class ByteBufferWritableChannel implements WritableByteChannel {

    private final byte[] data;
    private int offset;

    public ByteBufferWritableChannel(int size) {
        this.data = new byte[size];
    }

    public byte[] getData() {
        return data;
    }

    /**
     * Reads from the given buffer into the internal byte array.
     */
    @Override
    public int write(ByteBuffer src) {
        return write(src, 0);
    }

    public int write(ByteBuffer src, int startOffset) {
        int position = src.position();
        int len = position - startOffset;
        src.position(startOffset);
        src.get(data, offset, len);
        offset += len;
        src.position(position);
        return position;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {

    }

}
