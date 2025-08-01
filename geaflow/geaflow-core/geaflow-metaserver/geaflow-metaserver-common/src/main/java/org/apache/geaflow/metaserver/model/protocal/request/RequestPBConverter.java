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

package org.apache.geaflow.metaserver.model.protocal.request;

import com.google.protobuf.ByteString;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.metaserver.model.protocal.MetaRequest;
import org.apache.geaflow.rpc.proto.MetaServer.ServiceRequestPb;

public class RequestPBConverter {

    public static ServiceRequestPb convert(MetaRequest request) {
        ByteString bytes = RpcMessageEncoder.encode(request);
        return ServiceRequestPb.newBuilder().setRequest(bytes).build();
    }

    public static MetaRequest convert(ServiceRequestPb request) {
        return RpcMessageEncoder.decode(request.getRequest());
    }


}
