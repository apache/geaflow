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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferDataReader implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferDataReader.class);
    private static final AtomicBoolean END = new AtomicBoolean(false);
    private static final int HEADER_LENGTH = 4;
    private final DataInputStream input;

    public InferDataReader(DataExchangeQueue dataExchangeQueue) {
        DataQueueInputStream dataQueueInputStream = new DataQueueInputStream(dataExchangeQueue);
        this.input = new DataInputStream(dataQueueInputStream);
    }

    public byte[] read() throws IOException {
        byte[] buffer = new byte[HEADER_LENGTH];
        int bytesNum;
        try {
            bytesNum = input.read(buffer);
        } catch (EOFException e) {
            LOGGER.error("read infer data fail", e);
            END.set(true);
            return null;
        }
        if (bytesNum < 0) {
            LOGGER.warn("read infer data size is {}", bytesNum);
            END.set(true);
            return null;
        }
        if (bytesNum < buffer.length) {
            input.readFully(buffer, bytesNum, buffer.length - bytesNum);
        }
        int len = fromInt32LE(buffer);
        byte[] data = new byte[len];
        input.readFully(data);
        return data;
    }

    private int fromInt32LE(byte[] data) {
        Preconditions.checkState(data.length == HEADER_LENGTH, String.format("read data header "
            + "size %d, must be %d", data.length, HEADER_LENGTH));
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer.getInt();
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
