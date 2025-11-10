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

package org.apache.geaflow.common.encoder;

import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.geaflow.common.serialize.SerializerFactory;

public class RpcMessageEncoder implements Serializable {

    public static <T> T decode(ByteString payload) {
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("Cannot decode null or empty ByteString payload");
        }
        return SerializerFactory.getKryoSerializer().deserialize(payload.newInput());
    }

    public static <T> ByteString encode(T request) {
        ByteString.Output output = ByteString.newOutput();
        SerializerFactory.getKryoSerializer().serialize(request, output);
        return output.toByteString();
    }
}
