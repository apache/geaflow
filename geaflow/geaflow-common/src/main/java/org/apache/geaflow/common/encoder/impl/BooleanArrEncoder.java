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

public class BooleanArrEncoder extends AbstractEncoder<boolean[]> {

    public static final BooleanArrEncoder INSTANCE = new BooleanArrEncoder();

    @Override
    public void encode(boolean[] data, OutputStream outputStream) throws IOException {
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
            return;
        }
        int lenToWrite = data.length + 1;
        if (lenToWrite < 0) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(MSG_ARR_TOO_BIG));
        }
        Encoders.INTEGER.encode(lenToWrite, outputStream);
        for (boolean datum : data) {
            Encoders.BOOLEAN.encode(datum, outputStream);
        }
    }

    @Override
    public boolean[] decode(InputStream inputStream) throws IOException {
        int flag = Encoders.INTEGER.decode(inputStream);
        if (flag == NULL) {
            return null;
        }
        int length = flag - 1;
        boolean[] arr = new boolean[length];
        for (int i = 0; i < length; i++) {
            arr[i] = Encoders.BOOLEAN.decode(inputStream);
        }
        return arr;
    }

}
