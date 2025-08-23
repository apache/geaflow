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

package org.apache.geaflow.shuffle.serialize;

import java.io.IOException;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;

public class EncoderRecordSerializer<T> extends AbstractRecordSerializer<T> {

    private final IEncoder<T> encoder;

    public EncoderRecordSerializer(IEncoder<T> encoder) {
        this.encoder = encoder;
    }

    @Override
    public void doSerialize(T value, boolean isRetract, OutBuffer.BufferBuilder outBuffer) {
        try {
            this.encoder.encode(value, outBuffer.getOutputStream());
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.shuffleSerializeError(e.getMessage()), e);
        }
    }

}
