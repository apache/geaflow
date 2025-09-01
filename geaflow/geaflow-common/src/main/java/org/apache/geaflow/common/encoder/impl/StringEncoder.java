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
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class StringEncoder extends AbstractEncoder<String> {

    public static final StringEncoder INSTANCE = new StringEncoder();

    private static final int NULL = 0;

    @Override
    public void encode(String data, OutputStream outputStream) throws IOException {
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
            return;
        }

        // the length we write is offset by one, because a length of zero indicates a null value
        int lenToWrite = data.length() + 1;
        if (lenToWrite < 0) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError("string is too long"));
        }

        byte[] bytes = data.getBytes();
        IntegerEncoder.INSTANCE.encode(bytes.length + 1, outputStream);
        outputStream.write(bytes);
    }

    @Override
    public String decode(InputStream inputStream) throws IOException {
        int length = Encoders.INTEGER.decode(inputStream);
        if (length == NULL) {
            return null;
        }

        byte[] bytes = new byte[length - 1];
        inputStream.read(bytes);
        return new String(bytes);
    }

}
