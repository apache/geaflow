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

package org.apache.geaflow.state.graph.encoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DefaultBytesEncoder extends BaseBytesEncoder implements IBytesEncoder {

    private final byte magicNumber = 0x01;

    @Override
    public byte[] combine(List<byte[]> listBytes) {
        int len = listBytes.stream().mapToInt(c -> c.length).sum();
        ByteBuffer bf = ByteBuffer.wrap(new byte[len + Short.BYTES * listBytes.size() + Byte.BYTES]);
        for (byte[] bytes : listBytes) {
            bf.put(bytes);
        }
        for (byte[] bytes : listBytes) {
            bf.putShort((short) bytes.length);
        }
        bf.put(combine((byte) listBytes.size(), magicNumber));
        return bf.array();
    }

    @Override
    public List<byte[]> split(byte[] bytes) {
        ByteBuffer bf = ByteBuffer.wrap(bytes);
        bf.position(bytes.length - Byte.BYTES);
        byte lastByte = bf.get();
        if (magicNumber != parseMagicNumber(lastByte)) {
            return null;
        }
        int size = lastByte >> BYTE_SHIFT;
        int pos = bytes.length - Short.BYTES * size - Byte.BYTES;
        short[] lenArray = new short[size];
        bf.position(pos);
        for (int i = 0; i < size; i++) {
            lenArray[i] = bf.getShort();
        }

        bf.position(0);
        List<byte[]> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] tmp = new byte[lenArray[i]];
            bf.get(tmp);
            list.add(tmp);
        }
        return list;
    }

    @Override
    public byte[] combine(List<byte[]> listBytes, byte[] delimiter) {
        int len = listBytes.stream().mapToInt(c -> c.length).sum();
        ByteBuffer bf = ByteBuffer.wrap(
            new byte[len + (delimiter.length + Short.BYTES) * listBytes.size() + Byte.BYTES]);
        for (byte[] bytes : listBytes) {
            bf.put(bytes);
            bf.put(delimiter);
        }
        for (byte[] bytes : listBytes) {
            bf.putShort((short) bytes.length);
        }
        bf.put(combine((byte) listBytes.size(), magicNumber));
        return bf.array();
    }

    @Override
    public List<byte[]> split(byte[] bytes, byte[] delimiter) {
        ByteBuffer bf = ByteBuffer.wrap(bytes);
        bf.position(bytes.length - Byte.BYTES);
        byte lastByte = bf.get();
        if (magicNumber != parseMagicNumber(lastByte)) {
            return null;
        }
        int size = lastByte >> BYTE_SHIFT;
        int pos = bytes.length - Short.BYTES * size - Byte.BYTES;
        short[] lenArray = new short[size];
        bf.position(pos);
        for (int i = 0; i < size; i++) {
            lenArray[i] = bf.getShort();
        }

        byte[] empty = new byte[delimiter.length];
        bf.position(0);
        List<byte[]> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] tmp = new byte[lenArray[i]];
            bf.get(tmp);
            list.add(tmp);
            bf.get(empty);
        }
        return list;
    }

    @Override
    public byte getMyMagicNumber() {
        return magicNumber;
    }
}
