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

public class LongEncoder extends AbstractEncoder<Long> {

    public static final LongEncoder INSTANCE = new LongEncoder();

    @Override
    public void encode(Long data, OutputStream outputStream) throws IOException {
        // if between 0 ~ 127, just write the byte
        if (data >= 0 && data < 128) {
            outputStream.write(data.intValue());
            return;
        }

        // write var long, takes 1 ~ 9 byte
        long value = data;
        int varInt = (int) (value & 0x7F);
        value >>>= 7;

        varInt |= 0x80;
        varInt |= ((value & 0x7F) << 8);
        value >>>= 7;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            return;
        }

        varInt |= (0x80 << 8);
        varInt |= ((value & 0x7F) << 16);
        value >>>= 7;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            outputStream.write(varInt >> 16);
            return;
        }

        varInt |= (0x80 << 16);
        varInt |= ((value & 0x7F) << 24);
        value >>>= 7;
        if (value == 0) {
            outputStream.write(varInt);
            outputStream.write(varInt >> 8);
            outputStream.write(varInt >> 16);
            outputStream.write(varInt >> 24);
            return;
        }

        varInt |= (0x80 << 24);
        long varLong = (varInt & 0xFFFFFFFFL);
        varLong |= (((value & 0x7F)) << 32);
        value >>>= 7;
        if (value == 0) {
            outputStream.write((int) varLong);
            outputStream.write((int) (varLong >> 8));
            outputStream.write((int) (varLong >> 16));
            outputStream.write((int) (varLong >> 24));
            outputStream.write((int) (varLong >> 32));
            return;
        }

        varLong |= (0x80L << 32);
        varLong |= (((value & 0x7F)) << 40);
        value >>>= 7;
        if (value == 0) {
            outputStream.write((int) varLong);
            outputStream.write((int) (varLong >> 8));
            outputStream.write((int) (varLong >> 16));
            outputStream.write((int) (varLong >> 24));
            outputStream.write((int) (varLong >> 32));
            outputStream.write((int) (varLong >> 40));
            return;
        }

        varLong |= (0x80L << 40);
        varLong |= (((value & 0x7F)) << 48);
        value >>>= 7;
        if (value == 0) {
            outputStream.write((int) varLong);
            outputStream.write((int) (varLong >> 8));
            outputStream.write((int) (varLong >> 16));
            outputStream.write((int) (varLong >> 24));
            outputStream.write((int) (varLong >> 32));
            outputStream.write((int) (varLong >> 40));
            outputStream.write((int) (varLong >> 48));
            return;
        }

        varLong |= (0x80L << 48);
        varLong |= (((value & 0x7F)) << 56);
        value >>>= 7;
        if (value == 0) {
            outputStream.write((int) varLong);
            outputStream.write((int) (varLong >> 8));
            outputStream.write((int) (varLong >> 16));
            outputStream.write((int) (varLong >> 24));
            outputStream.write((int) (varLong >> 32));
            outputStream.write((int) (varLong >> 40));
            outputStream.write((int) (varLong >> 48));
            outputStream.write((int) (varLong >> 56));
            return;
        }

        varLong |= (0x80L << 56);
        outputStream.write((int) varLong);
        outputStream.write((int) (varLong >> 8));
        outputStream.write((int) (varLong >> 16));
        outputStream.write((int) (varLong >> 24));
        outputStream.write((int) (varLong >> 32));
        outputStream.write((int) (varLong >> 40));
        outputStream.write((int) (varLong >> 48));
        outputStream.write((int) (varLong >> 56));
        outputStream.write((int) value);
    }

    @Override
    public Long decode(InputStream inputStream) throws IOException {
        int b = inputStream.read();
        long result = b & 0x7F;
        if ((b & 0x80) != 0) {
            b = inputStream.read();
            result |= (b & 0x7F) << 7;
            if ((b & 0x80) != 0) {
                b = inputStream.read();
                result |= (b & 0x7F) << 14;
                if ((b & 0x80) != 0) {
                    b = inputStream.read();
                    result |= (b & 0x7F) << 21;
                    if ((b & 0x80) != 0) {
                        b = inputStream.read();
                        result |= (long) (b & 0x7F) << 28;
                        if ((b & 0x80) != 0) {
                            b = inputStream.read();
                            result |= (long) (b & 0x7F) << 35;
                            if ((b & 0x80) != 0) {
                                b = inputStream.read();
                                result |= (long) (b & 0x7F) << 42;
                                if ((b & 0x80) != 0) {
                                    b = inputStream.read();
                                    result |= (long) (b & 0x7F) << 49;
                                    if ((b & 0x80) != 0) {
                                        b = inputStream.read();
                                        result |= (long) b << 56;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

}
