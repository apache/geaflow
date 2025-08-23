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

package org.apache.geaflow.dsl.common.binary;

import org.apache.geaflow.common.binary.BinaryOperations;
import org.apache.geaflow.common.binary.HeapBinaryObject;
import org.apache.geaflow.common.binary.IBinaryObject;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public class HeapWriterBuffer implements WriterBuffer {

    private HeapBinaryObject buffer;

    private int cursor;

    private int extendPoint = 0;

    @Override
    public void initialize(int initSize) {
        if (initSize <= 0) {
            throw new GeaFlowDSLException("Illegal init buffer size: " + initSize);
        }
        buffer = HeapBinaryObject.of(new byte[initSize]);
        this.cursor = 0;
    }

    @Override
    public void grow(int size) {
        byte[] newBuffer = new byte[buffer.size() + size];
        BinaryOperations.copyMemory(buffer, 0,
            newBuffer, 0, buffer.size());
        this.buffer = HeapBinaryObject.of(newBuffer);
    }

    @Override
    public void growTo(int targetSize) {
        int growSize = targetSize - getCapacity();
        if (growSize > 0) {
            grow(growSize);
        }
    }

    @Override
    public byte[] copyBuffer() {
        assert buffer != null;
        byte[] copy = new byte[extendPoint];
        BinaryOperations.copyMemory(buffer, 0, copy, 0, extendPoint);
        return copy;
    }

    @Override
    public int getCapacity() {
        return buffer.size();
    }

    @Override
    public void writeByte(byte b) {
        BinaryOperations.putByte(buffer, cursor, b);
        cursor += 1;
        checkCursorAfterWrite();
    }

    @Override
    public void writeInt(int v) {
        BinaryOperations.putInt(buffer, cursor, v);
        cursor += 4;
        checkCursorAfterWrite();
    }

    @Override
    public void writeIntAlign(int v) {
        BinaryOperations.putLong(buffer, cursor, 0L);
        BinaryOperations.putInt(buffer, cursor, v);
        cursor += 8;
        checkCursorAfterWrite();
    }

    @Override
    public void writeShort(short v) {
        BinaryOperations.putShort(buffer, cursor, v);
        cursor += 2;
        checkCursorAfterWrite();
    }

    @Override
    public void writeShortAlign(short v) {
        BinaryOperations.putLong(buffer, cursor, 0L);
        BinaryOperations.putShort(buffer, cursor, v);
        cursor += 8;
        checkCursorAfterWrite();
    }

    @Override
    public void writeLong(long v) {
        BinaryOperations.putLong(buffer, cursor, v);
        cursor += 8;
        checkCursorAfterWrite();
    }

    @Override
    public void writeDouble(double v) {
        BinaryOperations.putDouble(buffer, cursor, v);
        cursor += 8;
        checkCursorAfterWrite();
    }

    @Override
    public void writeBytes(byte[] bytes) {
        BinaryOperations.copyMemory(bytes, 0, buffer, cursor, bytes.length);
        cursor += bytes.length;
        checkCursorAfterWrite();
    }

    @Override
    public void writeBytes(IBinaryObject src, long srcOffset, long length) {
        BinaryOperations.copyMemory(src, srcOffset, buffer, cursor, length);
        cursor += length;
        checkCursorAfterWrite();
    }

    private void checkCursorAfterWrite() {
        if (cursor <= 0 || cursor > buffer.size()) {
            throw new GeaFlowDSLException("Illegal cursor: " + cursor + ", buffer length is:" + buffer.size());
        }
    }

    @Override
    public int getCursor() {
        return cursor;
    }

    @Override
    public void setCursor(int cursor) {
        if (cursor < 0) {
            throw new GeaFlowDSLException("Illegal cursor" + cursor);
        }
        this.cursor = cursor;
    }

    @Override
    public void moveCursor(int cursor) {
        setCursor(getCursor() + cursor);
    }

    @Override
    public void setExtendPoint(int tailPoint) {
        if (tailPoint < this.extendPoint) {
            throw new GeaFlowDSLException("Current tailPoint: " + tailPoint + " should >= "
                + "the pre value:" + this.extendPoint);
        }
        this.extendPoint = tailPoint;
        if (tailPoint > buffer.size()) {
            grow(tailPoint - buffer.size());
        }
    }

    @Override
    public int getExtendPoint() {
        return extendPoint;
    }

    @Override
    public void moveToExtend() {
        setCursor(getExtendPoint());
    }

    @Override
    public void reset() {
        cursor = 0;
        extendPoint = 0;
    }

    @Override
    public void setNullAt(long offset, int index) {
        BinaryLayoutHelper.set(buffer, offset, index);
        // align index
        this.cursor += 8;
    }

    @Override
    public void release() {
        buffer.release();
    }

    @Override
    public boolean isReleased() {
        return buffer.isReleased();
    }
}
