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

package org.apache.geaflow.cluster.rpc;

import com.google.protobuf.Empty;
import org.apache.geaflow.rpc.proto.Master.HeartbeatRequest;
import org.apache.geaflow.rpc.proto.Master.HeartbeatResponse;
import org.apache.geaflow.rpc.proto.Master.RegisterRequest;
import org.apache.geaflow.rpc.proto.Master.RegisterResponse;

public interface IMasterEndpoint {

    /**
     * Register container into master.
     */
    RegisterResponse registerContainer(RegisterRequest request);

    /**
     * Register driver into master.
     */
    RegisterResponse registerDriver(RegisterRequest request);

    /**
     * Receive heart beat.
     */
    HeartbeatResponse receiveHeartbeat(HeartbeatRequest request);

    /**
     * Receive exception.
     */
    Empty receiveException(HeartbeatRequest request);

    /**
     * Close master.
     */
    Empty close(Empty request);
}

