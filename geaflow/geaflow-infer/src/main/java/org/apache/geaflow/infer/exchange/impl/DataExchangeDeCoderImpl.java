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

import java.io.IOException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.exchange.IDecoder;
import org.apache.geaflow.infer.exchange.serialize.Unpickler;

public class DataExchangeDeCoderImpl<T> implements IDecoder<T> {

    private final transient Unpickler unpickler;

    public DataExchangeDeCoderImpl() {
        this.unpickler = new Unpickler();
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            Object res = unpickler.loads(bytes);
            return (T) res;
        } catch (Exception e) {
            throw new GeaflowRuntimeException("unpick object error", e);
        }
    }

    @Override
    public void close() throws IOException {
        unpickler.close();
    }
}
