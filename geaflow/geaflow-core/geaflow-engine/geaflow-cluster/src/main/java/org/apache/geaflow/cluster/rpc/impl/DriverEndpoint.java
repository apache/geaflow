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

package org.apache.geaflow.cluster.rpc.impl;

import com.google.protobuf.Empty;
import org.apache.geaflow.cluster.driver.IDriver;
import org.apache.geaflow.cluster.rpc.IDriverEndpoint;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.rpc.proto.Driver.PipelineReq;
import org.apache.geaflow.rpc.proto.Driver.PipelineRes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverEndpoint implements IDriverEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverEndpoint.class);

    private final IDriver driver;

    public DriverEndpoint(IDriver driver) {
        this.driver = driver;
    }

    @Override
    public PipelineRes executePipeline(PipelineReq request) {
        try {
            Pipeline pipeline = RpcMessageEncoder.decode(request.getPayload());
            Object result = driver.executePipeline(pipeline);
            return PipelineRes.newBuilder()
                .setPayload(RpcMessageEncoder.encode(result))
                .build();
        } catch (Throwable e) {
            LOGGER.error("execute pipeline failed: {}", e.getMessage(), e);
            throw new GeaflowRuntimeException(String.format("execute pipeline failed: %s",
                e.getMessage()), e);
        }
    }

    @Override
    public Empty close(Empty request) {
        try {
            driver.close();
            return Empty.newBuilder().build();
        } catch (Throwable t) {
            LOGGER.error("close failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("close failed: %s", t.getMessage(), t));
        }
    }
}
