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

package org.apache.geaflow.common.encoder.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IntegerEncoder extends AbstractEncoder<Integer> {

    public static final IntegerEncoder INSTANCE = new IntegerEncoder();

    // VarInt encoding constants
    private static final int VARINT_MASK = 0x7F;
    private static final int VARINT_CONTINUE_FLAG = 0x80;
    private static final int VARINT_SHIFT = 7;
    private static final int DIRECT_WRITE_THRESHOLD = 128;

    @Override
    public void encode(Integer data, OutputStream outputStream) throws IOException {
        // if between 0 ~ 127, just write the byte
        if (data >= 0 && data < DIRECT_WRITE_THRESHOLD) {
            outputStream.write(data);
            return;
        }

        // write var int, takes 1 ~ 5 byte
        int value = data;
        int varInt = (value & VARINT_MASK);
        value >>>= VARINT_SHIFT;

        varInt |= VARINT_CONTINUE_FLAG;
        varInt |= ((value & VARINT_MASK) << 8);
        value >>>= VARINT_SHIFT;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            return;
        }

        varInt |= (VARINT_CONTINUE_FLAG << 8);
        varInt |= ((value & VARINT_MASK) << 16);
        value >>>= VARINT_SHIFT;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            outputStream.write(varInt >> 16);
            return;
        }

        varInt |= (VARINT_CONTINUE_FLAG << 16);
        varInt |= ((value & VARINT_MASK) << 24);
        value >>>= VARINT_SHIFT;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            outputStream.write(varInt >> 16);
            outputStream.write(varInt >> 24);
            return;
        }

        varInt |= (VARINT_CONTINUE_FLAG << 24);
        outputStream.write(varInt);
        outputStream.write(varInt >> 8);
        outputStream.write(varInt >> 16);
        outputStream.write(varInt >> 24);
        outputStream.write(value);
    }

    @Override
    public Integer decode(InputStream inputStream) throws IOException {
        int b = inputStream.read();
        int result = b & VARINT_MASK;
        if ((b & VARINT_CONTINUE_FLAG) != 0) {
            b = inputStream.read();
            result |= (b & VARINT_MASK) << VARINT_SHIFT;
            if ((b & VARINT_CONTINUE_FLAG) != 0) {
                b = inputStream.read();
                result |= (b & VARINT_MASK) << (VARINT_SHIFT * 2);
                if ((b & VARINT_CONTINUE_FLAG) != 0) {
                    b = inputStream.read();
                    result |= (b & VARINT_MASK) << (VARINT_SHIFT * 3);
                    if ((b & VARINT_CONTINUE_FLAG) != 0) {
                        b = inputStream.read();
                        result |= (b & VARINT_MASK) << (VARINT_SHIFT * 4);
                    }
                }
            }
        }
        return result;
    }

}
