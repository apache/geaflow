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
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.tuple.Triple;

public class TripleEncoder<F0, F1, F2> extends AbstractEncoder<Triple<F0, F1, F2>> {

    private final IEncoder<F0> encoder0;
    private final IEncoder<F1> encoder1;
    private final IEncoder<F2> encoder2;

    public TripleEncoder(IEncoder<F0> encoder0, IEncoder<F1> encoder1, IEncoder<F2> encoder2) {
        this.encoder0 = encoder0;
        this.encoder1 = encoder1;
        this.encoder2 = encoder2;
    }

    @Override
    public void init(Configuration config) {
        this.encoder0.init(config);
        this.encoder1.init(config);
        this.encoder2.init(config);
    }

    @Override
    public void encode(Triple<F0, F1, F2> data,
                       OutputStream outputStream) throws IOException {
        int flag = data == null ? NULL : NOT_NULL;
        Encoders.INTEGER.encode(flag, outputStream);
        if (flag == NULL) {
            return;
        }
        this.encoder0.encode(data.f0, outputStream);
        this.encoder1.encode(data.f1, outputStream);
        this.encoder2.encode(data.f2, outputStream);
    }

    @Override
    public Triple<F0, F1, F2> decode(InputStream inputStream) throws IOException {
        int flag = Encoders.INTEGER.decode(inputStream);
        if (flag == NULL) {
            return null;
        }
        F0 f0 = this.encoder0.decode(inputStream);
        F1 f1 = this.encoder1.decode(inputStream);
        F2 f2 = this.encoder2.decode(inputStream);
        return Triple.of(f0, f1, f2);
    }

}
