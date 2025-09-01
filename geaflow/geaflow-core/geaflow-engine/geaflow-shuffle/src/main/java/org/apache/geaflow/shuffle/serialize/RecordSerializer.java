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

import com.esotericsoftware.kryo.io.Output;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.serialize.impl.KryoSerializer;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer.BufferBuilder;

public class RecordSerializer<T> extends AbstractRecordSerializer<T> {

    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final Output output;
    private final KryoSerializer kryoSerializer;

    public RecordSerializer() {
        this.output = new Output(DEFAULT_BUFFER_SIZE);
        this.kryoSerializer = ((KryoSerializer) SerializerFactory.getKryoSerializer());
    }

    @Override
    public void doSerialize(T value, boolean isRetract, BufferBuilder outBuffer) {
        Output output = this.output;
        output.setOutputStream(outBuffer.getOutputStream());
        try {
            this.kryoSerializer.getThreadKryo().writeClassAndObject(output, value);
            output.flush();
        } finally {
            output.clear();
        }
    }

}
