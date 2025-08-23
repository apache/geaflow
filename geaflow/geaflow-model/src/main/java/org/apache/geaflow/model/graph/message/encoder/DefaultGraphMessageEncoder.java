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
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.model.graph.message.DefaultGraphMessage;

public class DefaultGraphMessageEncoder<K, M> extends AbstractGraphMessageEncoder<K, M, DefaultGraphMessage<K, M>> {

    public DefaultGraphMessageEncoder(IEncoder<K> keyEncoder, IEncoder<M> msgEncoder) {
        super(keyEncoder, msgEncoder);
    }

    @Override
    public void doEncode(DefaultGraphMessage<K, M> data, OutputStream outputStream) throws IOException {
        this.keyEncoder.encode(data.getTargetVId(), outputStream);
        this.msgEncoder.encode(data.getMessage(), outputStream);
    }

    @Override
    public DefaultGraphMessage<K, M> decode(InputStream inputStream) throws IOException {
        K key = this.keyEncoder.decode(inputStream);
        M msg = this.msgEncoder.decode(inputStream);
        return new DefaultGraphMessage<>(key, msg);
    }

}
