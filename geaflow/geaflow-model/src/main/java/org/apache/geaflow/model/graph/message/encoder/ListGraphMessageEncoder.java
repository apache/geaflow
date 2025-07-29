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

package org.apache.geaflow.model.graph.message.encoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.model.graph.message.ListGraphMessage;

public class ListGraphMessageEncoder<K, M> extends AbstractGraphMessageEncoder<K, M, ListGraphMessage<K, M>> {

    public ListGraphMessageEncoder(IEncoder<K> keyEncoder, IEncoder<M> msgEncoder) {
        super(keyEncoder, msgEncoder);
    }

    @Override
    public void doEncode(ListGraphMessage<K, M> data, OutputStream outputStream) throws IOException {
        this.keyEncoder.encode(data.getTargetVId(), outputStream);
        List<M> messages = data.getMessages();
        Encoders.INTEGER.encode(messages.size(), outputStream);
        for (M msg : messages) {
            this.msgEncoder.encode(msg, outputStream);
        }
    }

    @Override
    public ListGraphMessage<K, M> decode(InputStream inputStream) throws IOException {
        K vid = this.keyEncoder.decode(inputStream);
        int size = Encoders.INTEGER.decode(inputStream);
        List<M> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            M msg = this.msgEncoder.decode(inputStream);
            messages.add(msg);
        }
        return new ListGraphMessage<>(vid, messages);
    }

}
