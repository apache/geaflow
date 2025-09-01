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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.common.binary;

import static org.apache.geaflow.common.binary.BinaryOperations.copyMemory;
import static org.apache.geaflow.common.binary.BinaryOperations.getLong;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * This class is an adaptation of Spark's org.apache.spark.unsafe.types.UTF8String.
 */
public class BinaryString implements Comparable<BinaryString>, Serializable, KryoSerializable {

    private static final boolean IS_LITTLE_ENDIAN =
        ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private IBinaryObject binaryObject;
    private long offset;
    private int numBytes;

    private transient int hashCode = 0;

    private static byte[] bytesOfCodePointInUTF8 = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x00..0x0F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x10..0x1F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x20..0x2F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x30..0x3F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x40..0x4F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x50..0x5F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x60..0x6F
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x70..0x7F
        // Continuation bytes cannot appear as the first byte
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x80..0x8F
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x90..0x9F
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xA0..0xAF
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xB0..0xBF
        0, 0, // 0xC0..0xC1 - disallowed in UTF-8
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xC2..0xCF
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xD0..0xDF
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xE0..0xEF
        4, 4, 4, 4, 4, // 0xF0..0xF4
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // 0xF5..0xFF - disallowed in UTF-8
    };

    public static final BinaryString EMPTY_STRING = BinaryString.fromString("");

    public BinaryString() {

    }

    public BinaryString(IBinaryObject binaryObject, long offset, int numBytes) {
        this.binaryObject = binaryObject;
        this.offset = offset;
        this.numBytes = numBytes;
    }

    public static BinaryString fromString(String string) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        return new BinaryString(HeapBinaryObject.of(bytes), 0, bytes.length);
    }

    public static BinaryString fromBytes(byte[] bytes) {
        return new BinaryString(HeapBinaryObject.of(bytes), 0, bytes.length);
    }

    public byte[] getBytes() {
        if (offset == 0
            && binaryObject instanceof HeapBinaryObject
            && (((HeapBinaryObject) binaryObject).getBaseObject()).length == numBytes) {
            return ((HeapBinaryObject) binaryObject).getBaseObject();
        }
        byte[] bytes = new byte[numBytes];
        copyMemory(binaryObject, offset, bytes, 0, numBytes);
        return bytes;
    }

    @Override
    public String toString() {
        return new String(binaryObject.toBytes(), (int) offset, numBytes, StandardCharsets.UTF_8);
    }

    public IBinaryObject getBinaryObject() {
        return binaryObject;
    }

    public long getOffset() {
        return offset;
    }

    public int getNumBytes() {
        return numBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinaryString)) {
            return false;
        }
        BinaryString that = (BinaryString) o;
        if (numBytes != that.numBytes) {
            return false;
        }
        return BinaryOperations.arrayEquals(binaryObject, offset, that.binaryObject, that.offset, numBytes);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = hashUnsafeBytes(binaryObject, offset, numBytes, 42);
        }
        return hashCode;
    }

    @Override
    public int compareTo(BinaryString other) {
        int len = Math.min(numBytes, other.numBytes);
        int wordMax = (len / 8) * 8;
        long otherOffset = other.offset;
        IBinaryObject otherBase = other.binaryObject;
        for (int i = 0; i < wordMax; i += 8) {
            long left = getLong(binaryObject, offset + i);
            long right = getLong(otherBase, otherOffset + i);
            if (left != right) {
                if (IS_LITTLE_ENDIAN) {
                    // Use binary search
                    int n = 0;
                    int y;
                    long diff = left ^ right;
                    int x = (int) diff;
                    if (x == 0) {
                        x = (int) (diff >>> 32);
                        n = 32;
                    }

                    y = x << 16;
                    if (y == 0) {
                        n += 16;
                    } else {
                        x = y;
                    }

                    y = x << 8;
                    if (y == 0) {
                        n += 8;
                    }
                    return (int) (((left >>> n) & 0xFFL) - ((right >>> n) & 0xFFL));
                } else {
                    return Long.compareUnsigned(left, right);
                }
            }
        }
        for (int i = wordMax; i < len; i++) {
            // In UTF-8, the byte should be unsigned, so we should compare them as unsigned int.
            int res = (getByte(i) & 0xFF) - (BinaryOperations.getByte(otherBase, otherOffset + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return numBytes - other.numBytes;
    }

    public byte getByte(int i) {
        return BinaryOperations.getByte(binaryObject, offset + i);
    }

    public BinaryString[] split(BinaryString pattern, int limit) {
        // Java String's split method supports "ignore empty string" behavior when the limit is 0
        // whereas other languages do not. To avoid this java specific behavior, we fall back to
        // -1 when the limit is 0.
        if (limit == 0) {
            limit = -1;
        }
        String[] splits = toString().split(pattern.toString(), limit);
        BinaryString[] res = new BinaryString[splits.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = fromString(splits[i]);
        }
        return res;
    }

    /**
     * Get the length of chars in this string.
     */
    public int getLength() {
        int len = 0;
        for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
            len += 1;
        }
        return len;
    }

    public boolean contains(final BinaryString substring) {
        if (substring.numBytes == 0) {
            return true;
        }

        byte first = substring.getByte(0);
        for (int i = 0; i <= numBytes - substring.numBytes; i++) {
            if (getByte(i) == first && matchAt(substring, i)) {
                return true;
            }
        }
        return false;
    }

    public static BinaryString concat(BinaryString... inputs) {
        long totalLength = 0;
        for (BinaryString input : inputs) {
            if (Objects.isNull(input)) {
                continue;
            }
            totalLength += input.numBytes;
        }

        byte[] result = new byte[Math.toIntExact(totalLength)];
        int offset = 0;
        for (BinaryString input : inputs) {
            if (Objects.isNull(input)) {
                continue;
            }
            int len = input.numBytes;
            copyMemory(input.binaryObject, input.offset, result, offset, len);
            offset += len;
        }
        return fromBytes(result);
    }

    public static BinaryString concatWs(BinaryString separator, BinaryString... inputs) {
        if (Objects.isNull(separator)) {
            separator = EMPTY_STRING;
        }

        // total number of bytes from inputs
        long numInputBytes = 0L;
        int numInputs = inputs.length;
        for (BinaryString input : inputs) {
            if (Objects.nonNull(input)) {
                numInputBytes += input.numBytes;
            }
        }

        int resultSize =
            Math.toIntExact(numInputBytes + (numInputs - 1) * (long) separator.numBytes);
        byte[] result = new byte[resultSize];
        int offset = 0;

        for (int i = 0, j = 0; i < inputs.length; i++) {
            if (Objects.nonNull(inputs[i])) {
                int len = inputs[i].numBytes;
                copyMemory(inputs[i].binaryObject, inputs[i].offset, result,
                    offset, len);
                offset += len;
            }

            j++;
            // Add separator if this is not the last input.
            if (j < numInputs) {
                copyMemory(separator.binaryObject, separator.offset, result,
                    offset, separator.numBytes);
                offset += separator.numBytes;
            }
        }
        return fromBytes(result);
    }

    public int indexOf(BinaryString s, int start) {
        if (s.numBytes == 0) {
            return 0;
        }

        // locate to the start position.
        int i = 0; // position in byte
        int c = 0; // position in character
        while (i < numBytes && c < start) {
            i += numBytesForFirstByte(getByte(i));
            c += 1;
        }

        do {
            if (i + s.numBytes > numBytes) {
                return -1;
            }
            if (BinaryOperations.arrayEquals(binaryObject, offset + i, s.binaryObject, s.offset, s.numBytes)) {
                return c;
            }
            i += numBytesForFirstByte(getByte(i));
            c += 1;
        } while (i < numBytes);

        return -1;
    }

    public boolean matchAt(final BinaryString s, int pos) {
        if (s.numBytes + pos > numBytes || pos < 0) {
            return false;
        }
        return BinaryOperations.arrayEquals(binaryObject, offset + pos,
            s.binaryObject, s.offset, s.numBytes);
    }

    public boolean startsWith(final BinaryString prefix) {
        return matchAt(prefix, 0);
    }

    public boolean endsWith(final BinaryString suffix) {
        return matchAt(suffix, numBytes - suffix.numBytes);
    }

    private static int numBytesForFirstByte(final byte b) {
        final int offset = b & 0xFF;
        byte numBytes = bytesOfCodePointInUTF8[offset];
        return (numBytes == 0) ? 1 : numBytes; // Skip the first byte disallowed in UTF-8
    }

    public BinaryString substring(final int start) {
        return substring(start, getLength());
    }

    /**
     * This method is an adaptation of Spark's BinaryString#substring.
     */
    public BinaryString substring(final int start, final int end) {
        if (end <= start || start >= numBytes) {
            return EMPTY_STRING;
        }

        int i = 0;
        int c = 0;
        while (i < numBytes && c < start) {
            i += numBytesForFirstByte(getByte(i));
            c += 1;
        }

        int j = i;
        while (i < numBytes && c < end) {
            i += numBytesForFirstByte(getByte(i));
            c += 1;
        }

        if (i > j) {
            byte[] bytes = new byte[i - j];
            copyMemory(binaryObject, offset + j, bytes, 0, i - j);
            return fromBytes(bytes);
        } else {
            return EMPTY_STRING;
        }
    }

    public BinaryString reverse() {
        byte[] bytes = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            bytes[i] = getByte(numBytes - i - 1);
        }
        return fromBytes(bytes);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(numBytes);
        output.write(binaryObject.toBytes(), (int) offset, numBytes);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int size = input.readInt();
        byte[] bytes = new byte[size];
        input.read(bytes);
        this.binaryObject = HeapBinaryObject.of(bytes);
        this.offset = 0;
        this.numBytes = bytes.length;
    }

    /**
     * * This method is an adaptation of Spark's org.apache.spark.unsafe.hash.Murmur3_x86_32.
     */
    private static int hashUnsafeBytes(IBinaryObject base, long offset, int lengthInBytes, int seed) {
        int lengthAligned = lengthInBytes - lengthInBytes % 4;
        int h1 = hashBytesByInt(base, offset, lengthAligned, seed);
        for (int i = lengthAligned; i < lengthInBytes; i++) {
            int halfWord = BinaryOperations.getByte(base, offset + i);
            int k1 = mixK1(halfWord);
            h1 = mixH1(h1, k1);
        }
        return fmix(h1, lengthInBytes);
    }

    private static int hashBytesByInt(IBinaryObject base, long offset, int lengthInBytes, int seed) {
        assert (lengthInBytes % 4 == 0);
        int h1 = seed;
        for (int i = 0; i < lengthInBytes; i += 4) {
            int halfWord = BinaryOperations.getInt(base, offset + i);
            if (!IS_LITTLE_ENDIAN) {
                halfWord = Integer.reverseBytes(halfWord);
            }
            h1 = mixH1(h1, mixK1(halfWord));
        }
        return h1;
    }

    private static int mixK1(int k1) {
        k1 *= 0xcc9e2d51;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= 0x1b873593;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
