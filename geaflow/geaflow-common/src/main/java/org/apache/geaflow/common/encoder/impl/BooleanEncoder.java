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

public class BooleanEncoder extends AbstractEncoder<Boolean> {

    public static final BooleanEncoder INSTANCE = new BooleanEncoder();

    private static final int TRUE = 1;
    private static final int FALSE = 2;

    @Override
    public void encode(Boolean data, OutputStream outputStream) throws IOException {
        // 0: null
        // 1: true
        // 2: false
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
        } else if (data) {
            Encoders.INTEGER.encode(TRUE, outputStream);
        } else {
            Encoders.INTEGER.encode(FALSE, outputStream);
        }
    }

    @Override
    public Boolean decode(InputStream inputStream) throws IOException {
        Integer value = Encoders.INTEGER.decode(inputStream);
        if (value == NULL) {
            return null;
        }
        return value == TRUE;
    }

}
