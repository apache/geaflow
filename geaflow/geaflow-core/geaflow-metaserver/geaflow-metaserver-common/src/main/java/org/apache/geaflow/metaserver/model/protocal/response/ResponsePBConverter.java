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

package org.apache.geaflow.metaserver.model.protocal.response;

import com.google.protobuf.ByteString;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.metaserver.model.protocal.MetaResponse;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceResultPb;

public class ResponsePBConverter {

    public static ServiceResultPb convert(MetaResponse response) {
        ByteString bytes = RpcMessageEncoder.encode(response);
        return ServiceResultPb.newBuilder().setResult(bytes).build();
    }

    public static MetaResponse convert(ServiceResultPb result) {
        return RpcMessageEncoder.decode(result.getResult());
    }
}
