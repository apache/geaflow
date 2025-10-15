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

import static org.apache.geaflow.common.binary.BinaryOperations.getByte;
import static org.apache.geaflow.common.binary.BinaryOperations.putByte;
import static org.apache.geaflow.common.binary.BinaryOperations.putLong;

import org.apache.geaflow.common.binary.BinaryOperations;
import org.apache.geaflow.common.binary.HeapBinaryObject;
import org.apache.geaflow.common.binary.IBinaryObject;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

/**
 * This is based on Spark's BitSetMethods.
 */
public class BinaryLayoutHelper {

    public static final int FIELDS_NUM_OFFSET = 0;

    public static final int NULL_BIT_OFFSET = 4;

    public static int getFieldsNum(IBinaryObject binaryObject) {
        return BinaryOperations.getInt(binaryObject, FIELDS_NUM_OFFSET);
    }

    /**
     * Get the number bytes need to store null bit.
     *
     * @param numValues Number of values.
     */
    public static int getBitSetBytes(int numValues) {
        return (numValues + 7) / 8;
    }

    public static int getExtendPoint(int numValues) {
        return NULL_BIT_OFFSET + getBitSetBytes(numValues) + numValues * 8;
    }

    /**
     * Set the index-th bit to 1 from the baseOffset.
     *
     * @param baseObject The baseObject to set, it may be a byte[] for heap memory or null for off-heap.
     * @param baseOffset The base offset.
     * @param index      The index-th bit to set to 1.
     */
    public static void set(IBinaryObject baseObject, long baseOffset, int index) {
        if (index < 0) {
            throw new GeaFlowDSLException("index (" + index + ") should >= 0");
        }
        final long mask = 1L << (index & 0x7);  // mod 8 and shift
        final long wordOffset = baseOffset + (index >> 3); // div 8
        final byte word = getByte(baseObject, wordOffset);
        putByte(baseObject, wordOffset, (byte) (word | mask));
    }

    public static void unset(IBinaryObject baseObject, long baseOffset, int index) {
        if (index < 0) {
            throw new GeaFlowDSLException("index (" + index + ") should >= 0");
        }
        final long mask = 1L << (index & 0x7);  // mod 8 and shift
        final long wordOffset = baseOffset + (index >> 3);
        final byte word = getByte(baseObject, wordOffset);
        putByte(baseObject, wordOffset, (byte) (word & ~mask));
    }

    public static boolean isSet(IBinaryObject baseObject, long baseOffset, int index) {
        if (index < 0) {
            throw new GeaFlowDSLException("index (" + index + ") should >= 0");
        }
        final long mask = 1L << (index & 0x7);  // mod 8 and shift
        final long wordOffset = baseOffset + (index >> 3);
        final byte word = getByte(baseObject, wordOffset);
        return (word & mask) != 0;
    }

    public static long getFieldOffset(int nullBitSetBytes, int index) {
        return NULL_BIT_OFFSET + nullBitSetBytes + index * 8L;
    }

    public static long getArrayFieldOffset(int nullBitSetBytes, int index) {
        return nullBitSetBytes + index * 8L;
    }

    public static void zeroBytes(byte[] bytes) {
        zeroBytes(bytes, 0, bytes.length);
    }

    public static void zeroBytes(byte[] bytes, long baseOffset, int size) {
        zeroBytes(HeapBinaryObject.of(bytes), baseOffset, size);
    }

    public static void zeroBytes(IBinaryObject baseObject, long baseOffset, int size) {
        assert baseOffset >= 0 && baseOffset + size <= baseObject.size();

        int workAlign = size / 8 * 8;
        for (int i = 0; i < workAlign; i += 8) {
            putLong(baseObject, baseOffset + i, 0L);
        }
        int retain = size - workAlign;
        for (int i = 0; i < retain; i++) {
            putByte(baseObject, baseOffset + workAlign + i, (byte) 0);
        }
    }

    public static int getInitBufferSize(int fieldsNum) {
        return NULL_BIT_OFFSET + fieldsNum * 8;
    }
}
