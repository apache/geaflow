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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import sun.misc.Unsafe;

public class DataQueueOutputStream extends OutputStream {
    private static final int BUFFER_SIZE = 10 * 1024;
    private final DataExchangeQueue dataExchangeQueue;
    private final byte[] dataBufferArray;
    private final ByteBuffer buffer;

    private final int queueCapacity;
    private final int queueMask;

    public DataQueueOutputStream(DataExchangeQueue dataExchangeQueue) {
        this.dataExchangeQueue = dataExchangeQueue;
        this.queueCapacity = dataExchangeQueue.getQueueCapacity();
        this.dataBufferArray = new byte[BUFFER_SIZE];
        this.queueMask = dataExchangeQueue.getQueueMask();
        this.buffer = ByteBuffer.wrap(dataBufferArray, 0, dataBufferArray.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void write(int b) throws IOException {
        dataBufferArray[0] = (byte) (b & 0xff);
        write(dataBufferArray, 0, 1);
    }

    @Override
    public void write(byte[] buffer, int offset, int size) throws IOException {
        long outputPointer = dataExchangeQueue.getOutputPointer();
        long currentInputIndex = outputPointer - (queueCapacity - size);
        while (dataExchangeQueue.getInputNextPointer() <= currentInputIndex
            || dataExchangeQueue.getBarrierAddress() > dataExchangeQueue.getCurrentBufferAddress()) {

            dataExchangeQueue.setInputNextPointer(dataExchangeQueue.getInputPointerByVolatile());
            if (dataExchangeQueue.getInputNextPointer() <= currentInputIndex
                || dataExchangeQueue.getBarrierAddress() > dataExchangeQueue.getCurrentBufferAddress()) {
                if (dataExchangeQueue.enableFinished()) {
                    throw new GeaflowRuntimeException("output queue is marked finished");
                }
                Thread.yield();
            }
        }

        int currentOutputNum = 0;
        while (currentOutputNum < size) {
            long nextPointIndex = getNextPointIndex(outputPointer, queueCapacity);
            int remainNum = (int) (nextPointIndex - outputPointer);
            int bytesToWrite = Math.min(size - currentOutputNum, remainNum);
            int left = Unsafe.ARRAY_BYTE_BASE_OFFSET + offset + currentOutputNum;
            long right = dataExchangeQueue.getInitialQueueAddress() + (outputPointer & queueMask);

            UnSafeUtils.UNSAFE.copyMemory(buffer, left, null, right, bytesToWrite);
            dataExchangeQueue.setOutputPointer(outputPointer + bytesToWrite);
            currentOutputNum += bytesToWrite;
            outputPointer += bytesToWrite;
        }
        dataExchangeQueue.setOutputPointer(outputPointer);
    }

    public boolean tryReserveBeforeWrite(int len) {
        long outputPointer = dataExchangeQueue.getOutputPointer();
        long currentInputIndex = outputPointer - (queueCapacity - len);
        if (dataExchangeQueue.getInputNextPointer() <= currentInputIndex) {
            dataExchangeQueue.setInputNextPointer(dataExchangeQueue.getInputPointerByVolatile());
        }
        long inputNextPointer = dataExchangeQueue.getInputNextPointer();
        return inputNextPointer > currentInputIndex;
    }

    @Override
    public void close() {
        dataExchangeQueue.markFinished();
    }
}

