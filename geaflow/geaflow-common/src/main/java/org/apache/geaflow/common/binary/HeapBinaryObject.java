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

package org.apache.geaflow.common.binary;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class HeapBinaryObject implements IBinaryObject {

    private byte[] bytes;

    public HeapBinaryObject() {

    }

    private HeapBinaryObject(byte[] bytes) {
        this.bytes = bytes;
    }

    public static HeapBinaryObject of(byte[] bytes) {
        return new HeapBinaryObject(bytes);
    }

    @Override
    public byte[] getBaseObject() {
        return bytes;
    }

    @Override
    public long getAbsoluteAddress(long address) {
        if (address < 0 || address >= size()) {
            throw new GeaflowRuntimeException("Illegal address: " + address + ", is out of visit "
                + "range[0," + size() + ")");
        }
        return BinaryOperations.BYTE_ARRAY_OFFSET + address;
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public void release() {
        bytes = null;
    }

    @Override
    public boolean isReleased() {
        return bytes == null;
    }

    @Override
    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HeapBinaryObject)) {
            return false;
        }
        HeapBinaryObject that = (HeapBinaryObject) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeVarInt(bytes.length, true);
        output.write(bytes);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int length = input.readVarInt(true);
        this.bytes = new byte[length];
        input.read(bytes);
    }

    @Override
    public String toString() {
        return "HeapBinaryObject{" + "bytes=" + Arrays.toString(bytes) + '}';
    }
}
