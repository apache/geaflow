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

package org.apache.geaflow.infer.exchange.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.geaflow.infer.exchange.DataExchangeContext;
import org.apache.geaflow.infer.exchange.DataExchangeQueue;
import org.apache.geaflow.infer.exchange.IDataBridge;
import org.apache.geaflow.infer.exchange.IDecoder;
import org.apache.geaflow.infer.exchange.IEncoder;
import org.apache.geaflow.infer.exchange.InferDataReader;
import org.apache.geaflow.infer.exchange.InferDataWriter;

public class InferDataBridgeImpl<OUT> implements IDataBridge<OUT> {

    private static final int HEADER_LENGTH = 4;
    private final byte[] bufferArray;
    private final ByteBuffer byteBuffer;
    private final InferDataWriter inferDataWriter;
    private final InferDataReader inferDataReader;
    private final IEncoder encoder;
    private final IDecoder<OUT> decoder;

    public InferDataBridgeImpl(DataExchangeContext shareMemoryContext) {
        DataExchangeQueue receiveQueue = shareMemoryContext.getReceiveQueue();
        DataExchangeQueue sendQueue = shareMemoryContext.getSendQueue();
        this.inferDataReader = new InferDataReader(receiveQueue);
        this.inferDataWriter = new InferDataWriter(sendQueue);
        this.encoder = new DataExchangeEnCoderImpl();
        this.decoder = new DataExchangeDeCoderImpl();
        this.bufferArray = new byte[HEADER_LENGTH];
        this.byteBuffer = ByteBuffer.wrap(bufferArray);
        this.byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public boolean write(Object... inputs) throws IOException {
        int inputsSize = inputs.length;
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] dataSizeBytes = toInt32LE(inputsSize);
        result.write(dataSizeBytes);
        for (Object element : inputs) {
            result.write(transformBytes(element));
        }
        byte[] byteArray = result.toByteArray();
        return inferDataWriter.write(byteArray);
    }

    @Override
    public OUT read() throws IOException {
        byte[] result = inferDataReader.read();
        if (result != null) {
            return this.decoder.decode(result);
        }
        return null;
    }

    private byte[] transformBytes(Object obj) {
        byte[] dataBytes = this.encoder.encode(obj);
        int dataLength = dataBytes.length;
        byte[] lenBytes = toInt32LE(dataLength);
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH + dataLength);
        buffer.put(lenBytes);
        buffer.put(dataBytes);
        return buffer.array();
    }

    private byte[] toInt32LE(int data) {
        this.byteBuffer.clear();
        this.byteBuffer.putInt(data);
        return bufferArray;
    }

    @Override
    public void close() throws IOException {
        inferDataReader.close();
        inferDataWriter.close();
        encoder.close();
        decoder.close();
    }
}
