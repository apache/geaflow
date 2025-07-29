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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jctools.util.PortableJvmInfo;
import org.jctools.util.Pow2;

public final class DataExchangeQueue implements Closeable {

    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    private final long outputNextAddress;
    private final long capacityAddress;
    private final long outputAddress;
    private final long inputNextAddress;
    private final long barrierAddress;
    private final long currentBufferAddress;
    private final long mapAddress;
    private final int queueCapacity;
    private final int bufferCapacity;
    private final long initialRawAddress;

    private final long startPointAddress;

    private final long endPointAddress;
    private final MemoryMapper memoryMapper;

    public DataExchangeQueue(String mapKey, int capacity, boolean reset) {
        this.bufferCapacity = getBufferCapacity(capacity);
        this.memoryMapper = new MemoryMapper(mapKey,
            bufferCapacity + PortableJvmInfo.CACHE_LINE_SIZE);
        this.mapAddress = memoryMapper.getMapAddress();
        this.queueCapacity = Pow2.roundToPowerOfTwo(capacity);
        this.initialRawAddress = Pow2.align(mapAddress, PortableJvmInfo.CACHE_LINE_SIZE);
        this.startPointAddress = initialRawAddress;
        this.capacityAddress = startPointAddress + PortableJvmInfo.CACHE_LINE_SIZE;
        this.outputAddress = startPointAddress + 2L * PortableJvmInfo.CACHE_LINE_SIZE;
        this.inputNextAddress = outputAddress + 8;
        this.outputNextAddress = startPointAddress + 8;
        this.endPointAddress = outputAddress + PortableJvmInfo.CACHE_LINE_SIZE;
        this.barrierAddress = endPointAddress + 8;
        this.currentBufferAddress = barrierAddress + 8;
        if (reset) {
            reset();
        }
    }


    @Override
    public synchronized void close() {
        CLOSED.set(true);
        if (memoryMapper != null) {
            memoryMapper.close();
        }
        UnSafeUtils.UNSAFE.freeMemory(mapAddress);
    }

    public long getMemoryMapSize() {
        if (memoryMapper == null) {
            return 0;
        }
        return memoryMapper.getMapSize();
    }

    public long getInputPointer() {
        return UnSafeUtils.UNSAFE.getLong(null, startPointAddress);
    }

    public long getInputPointerByVolatile() {
        return UnSafeUtils.UNSAFE.getLongVolatile(null, startPointAddress);
    }

    public void setInputPointer(long value) {
        UnSafeUtils.UNSAFE.putOrderedLong(null, startPointAddress, value);
    }

    public long getOutputPointer() {
        return UnSafeUtils.UNSAFE.getLong(null, outputAddress);
    }

    public long getOutputPointerByVolatile() {
        return UnSafeUtils.UNSAFE.getLongVolatile(null, outputAddress);
    }

    public void setOutputPointer(long value) {
        UnSafeUtils.UNSAFE.putOrderedLong(null, outputAddress, value);
    }

    public long getInputNextPointer() {
        return UnSafeUtils.UNSAFE.getLong(null, inputNextAddress);
    }

    public void setInputNextPointer(final long value) {
        UnSafeUtils.UNSAFE.putLong(inputNextAddress, value);
    }

    public long getOutputNextPointer() {
        return UnSafeUtils.UNSAFE.getLong(null, outputNextAddress);
    }

    public void setOutputNextPointer(final long value) {
        UnSafeUtils.UNSAFE.putLong(outputNextAddress, value);
    }

    public long getBarrierAddress() {
        return UnSafeUtils.UNSAFE.getLong(barrierAddress);
    }

    public long getCurrentBufferAddress() {
        return UnSafeUtils.UNSAFE.getLong(currentBufferAddress);
    }

    public boolean enableFinished() {
        return UnSafeUtils.UNSAFE.getLongVolatile(null, endPointAddress) != 0;
    }

    public synchronized void markFinished() {
        if (!CLOSED.get()) {
            UnSafeUtils.UNSAFE.putOrderedLong(null, endPointAddress, -1);
        }
    }

    public long getInitialQueueAddress() {
        return initialRawAddress + 4L * PortableJvmInfo.CACHE_LINE_SIZE;
    }

    public int getQueueMask() {
        return this.queueCapacity - 1;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public int getBufferCapacity(int capacity) {
        return (Pow2.roundToPowerOfTwo(capacity)) + 4 * PortableJvmInfo.CACHE_LINE_SIZE;
    }

    public void reset() {
        UnSafeUtils.UNSAFE.setMemory(initialRawAddress, bufferCapacity, (byte) 0);
        UnSafeUtils.UNSAFE.putLongVolatile(null, capacityAddress, queueCapacity);
    }

    public static long getNextPointIndex(long v, int capacity) {
        if ((v & (capacity - 1)) == 0) {
            return v + capacity;
        }
        return Pow2.align(v, capacity);
    }
}