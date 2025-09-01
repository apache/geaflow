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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class InferDataWriter implements Closeable {

    private static final int HEADER_LENGTH = 4;
    private final DataQueueOutputStream outputStream;
    private final byte[] dataHeaderBytes;
    private final ByteBuffer headerByteBuffer;

    public InferDataWriter(DataExchangeQueue dataExchangeQueue) {
        this.outputStream = new DataQueueOutputStream(dataExchangeQueue);
        this.dataHeaderBytes = new byte[HEADER_LENGTH];
        this.headerByteBuffer = ByteBuffer.wrap(dataHeaderBytes);
        this.headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public boolean write(byte[] record, int offset, int length) throws IOException {
        int outputSize = HEADER_LENGTH + (length - offset);
        if (!outputStream.tryReserveBeforeWrite((outputSize))) {
            return false;
        }
        byte[] headerData = extractHeaderData(length);
        outputStream.write(headerData, 0, HEADER_LENGTH);
        outputStream.write(record, offset, length);
        return true;
    }

    public boolean write(byte[] record) throws IOException {
        return write(record, 0, record.length);
    }

    private byte[] extractHeaderData(int data) {
        headerByteBuffer.clear();
        headerByteBuffer.putInt(data);
        return dataHeaderBytes;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
