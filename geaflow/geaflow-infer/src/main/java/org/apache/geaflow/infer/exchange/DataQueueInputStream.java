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

package org.apache.geaflow.infer.exchange;

import static org.apache.geaflow.infer.exchange.DataExchangeQueue.getNextPointIndex;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

public class DataQueueInputStream extends InputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataQueueInputStream.class);
    private static final int BUFFER_SIZE = 10 * 1024;
    private static final int INT_SIZE = 4;
    private static final int SHORT_SIZE = 2;
    private static final int LONG_SIZE = 8;
    private final DataExchangeQueue dataExchangeQueue;
    private final byte[] dataBufferArray;
    private final ByteBuffer buffer;
    private final int queueCapacity;
    private final long initialAddress;
    private final int queueMask;

    public DataQueueInputStream(DataExchangeQueue dataExchangeQueue) {
        this.dataExchangeQueue = dataExchangeQueue;
        this.queueCapacity = dataExchangeQueue.getQueueCapacity();
        this.initialAddress = dataExchangeQueue.getInitialQueueAddress();
        this.queueMask = dataExchangeQueue.getQueueMask();
        this.dataBufferArray = new byte[BUFFER_SIZE];
        this.buffer = ByteBuffer.wrap(dataBufferArray, 0, dataBufferArray.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public int read() throws IOException {
        int r = read(dataBufferArray, 0, 1);
        if (r == 1) {
            return dataBufferArray[0] & 0xFF;
        }
        return -1;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        int currentIndex = 0;
        while (currentIndex < length) {
            int currentLength;
            try {
                currentLength = readFully(buffer, currentIndex + offset, length - currentIndex);
            } catch (InterruptedException e) {
                InterruptedIOException interruptedIOException = new InterruptedIOException(e.getMessage());
                interruptedIOException.bytesTransferred = currentIndex;
                LOGGER.error("read infer data failed", e);
                throw interruptedIOException;
            }
            if (currentLength < 0) {
                return currentIndex > 0 ? currentIndex : -1;
            }
            currentIndex += currentLength;
        }
        return currentIndex;
    }

    public int read(byte[] b, int size) throws IOException {
        return read(b, 0, size);
    }

    private int readFully(byte[] buffer, int offset, int length) throws InterruptedException {
        long inputPointer = dataExchangeQueue.getInputPointer();
        long outputNextPointer = dataExchangeQueue.getOutputNextPointer();

        while (inputPointer >= outputNextPointer) {
            long outputPointer = dataExchangeQueue.getOutputPointerByVolatile();
            dataExchangeQueue.setOutputNextPointer(outputPointer);
            outputNextPointer = dataExchangeQueue.getOutputNextPointer();
            if (inputPointer >= outputNextPointer) {
                if (dataExchangeQueue.enableFinished()) {
                    long outputPointerByVolatile = dataExchangeQueue.getOutputPointerByVolatile();
                    dataExchangeQueue.setOutputNextPointer(outputPointerByVolatile);
                    outputNextPointer = dataExchangeQueue.getOutputNextPointer();
                    if (inputPointer >= outputNextPointer) {
                        return -1;
                    }
                    break;
                }
            }
        }
        long nextPointIndex = getNextPointIndex(inputPointer, queueCapacity);
        int remainByteNum;
        if (outputNextPointer > nextPointIndex) {
            remainByteNum = (int) (nextPointIndex - inputPointer);
        } else {
            remainByteNum = (int) (outputNextPointer - inputPointer);
        }
        int readableNum = Math.min(remainByteNum, length);
        long left = this.initialAddress + (inputPointer & this.queueMask);
        int right = Unsafe.ARRAY_BYTE_BASE_OFFSET + offset;
        UnSafeUtils.UNSAFE.copyMemory(null, left, buffer, right, readableNum);
        dataExchangeQueue.setInputPointer(inputPointer + readableNum);
        return readableNum;
    }

    @Override
    public int available() {
        final long currentRead = dataExchangeQueue.getInputPointer();
        long writeCache = dataExchangeQueue.getOutputNextPointer();
        if (currentRead >= writeCache) {
            dataExchangeQueue.setOutputNextPointer(dataExchangeQueue.getOutputPointerByVolatile());
            writeCache = dataExchangeQueue.getOutputNextPointer();
        }

        int availRead = (int) (writeCache - currentRead);
        if (availRead > 0) {
            return availRead;
        }
        return 0;
    }

    public int getInt() throws IOException {
        read(dataBufferArray, INT_SIZE);
        buffer.clear();
        return buffer.getInt();
    }

    public short getShort() throws IOException {
        read(dataBufferArray, SHORT_SIZE);
        buffer.clear();
        return buffer.getShort();
    }

    public long getLong() throws IOException {
        read(dataBufferArray, LONG_SIZE);
        buffer.clear();
        return buffer.getLong();
    }

    public double getDouble() throws IOException {
        read(dataBufferArray, LONG_SIZE);
        buffer.clear();
        return buffer.getDouble();
    }

    public float getFloat() throws IOException {
        read(dataBufferArray, INT_SIZE);
        buffer.clear();
        return buffer.getFloat();
    }
}