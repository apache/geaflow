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

package org.apache.geaflow.common.rpc;

import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.server.RpcServerOptions;
import com.baidu.brpc.utils.BrpcConstants;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableServerOption {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableServerOption.class);

    public static RpcServerOptions build(Configuration config) {
        RpcServerOptions serverOptions = new RpcServerOptions();
        serverOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        int maxRetryTimes = config.getInteger(ExecutionConfigKeys.RPC_MAX_RETRY_TIMES);
        serverOptions.setMaxTryTimes(maxRetryTimes);

        int keepAliveTime = config.getInteger(ExecutionConfigKeys.RPC_KEEP_ALIVE_TIME_SEC);
        serverOptions.setKeepAliveTime(keepAliveTime);

        boolean threadSharing = config.getBoolean(ExecutionConfigKeys.RPC_THREADPOOL_SHARING_ENABLE);
        serverOptions.setGlobalThreadPoolSharing(threadSharing);

        int ioThreadNum = config.getInteger(ExecutionConfigKeys.RPC_IO_THREAD_NUM);
        int workerThreadNum = config.getInteger(ExecutionConfigKeys.RPC_WORKER_THREAD_NUM);
        int defaultThreads = Math.max(Runtime.getRuntime().availableProcessors(), 8);

        serverOptions.setIoThreadNum(Math.max(ioThreadNum, defaultThreads));
        serverOptions.setWorkThreadNum(Math.max(workerThreadNum, defaultThreads));
        serverOptions.setIoEventType(BrpcConstants.IO_EVENT_NETTY_EPOLL);

        int rpcBufferSize = config.getInteger(ExecutionConfigKeys.RPC_BUFFER_SIZE_BYTES);
        serverOptions.setSendBufferSize(rpcBufferSize);
        serverOptions.setReceiveBufferSize(rpcBufferSize);

        LOGGER.info("server options set: {}", serverOptions);
        return serverOptions;
    }
}
