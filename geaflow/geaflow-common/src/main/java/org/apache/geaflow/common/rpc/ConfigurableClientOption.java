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

import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.utils.BrpcConstants;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableClientOption {

    private static final String SHORT_CONNECTION = "short_connection";
    private static final String SINGLE_CONNECTION = "single_connection";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableClientOption.class);

    public static RpcClientOptions build(Configuration config) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        int maxRetryTimes = config.getInteger(ExecutionConfigKeys.RPC_MAX_RETRY_TIMES);
        clientOption.setMaxTryTimes(maxRetryTimes);

        // only works for pooled connection
        int maxTotalConnectionNum = config.getInteger(ExecutionConfigKeys.RPC_MAX_TOTAL_CONNECTION_NUM);
        clientOption.setMaxTotalConnections(maxTotalConnectionNum);
        // only works for pooled connection
        int minIdleConnectionNum = config.getInteger(ExecutionConfigKeys.RPC_MIN_IDLE_CONNECTION_NUM);
        clientOption.setMinIdleConnections(minIdleConnectionNum);

        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

        boolean threadSharing = config.getBoolean(ExecutionConfigKeys.RPC_THREADPOOL_SHARING_ENABLE);
        clientOption.setGlobalThreadPoolSharing(threadSharing);

        int ioThreadNum = config.getInteger(ExecutionConfigKeys.RPC_IO_THREAD_NUM);
        int workerThreadNum = config.getInteger(ExecutionConfigKeys.RPC_WORKER_THREAD_NUM);
        int defaultThreads = Runtime.getRuntime().availableProcessors();

        clientOption.setIoThreadNum(Math.max(ioThreadNum, defaultThreads));
        clientOption.setWorkThreadNum(Math.max(workerThreadNum, defaultThreads));

        clientOption.setIoEventType(BrpcConstants.IO_EVENT_NETTY_EPOLL);
        int rpcBufferSize = config.getInteger(ExecutionConfigKeys.RPC_BUFFER_SIZE_BYTES);
        clientOption.setSendBufferSize(rpcBufferSize);
        clientOption.setReceiveBufferSize(rpcBufferSize);

        ChannelType channelType = getChannelType(config);
        if (ChannelType.SINGLE_CONNECTION.equals(channelType)) {
            // Only SINGLE_CONNECTION type needs to be set keepAliveTime.
            int keepAliveTime = config.getInteger(ExecutionConfigKeys.RPC_KEEP_ALIVE_TIME_SEC);
            clientOption.setKeepAliveTime(keepAliveTime);
        }
        clientOption.setChannelType(channelType);

        int writeTimeout = config.getInteger(ExecutionConfigKeys.RPC_WRITE_TIMEOUT_MS);
        clientOption.setWriteTimeoutMillis(writeTimeout);
        int readTimeout = config.getInteger(ExecutionConfigKeys.RPC_READ_TIMEOUT_MS);
        clientOption.setReadTimeoutMillis(readTimeout);
        int connectTimeout = config.getInteger(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS);
        clientOption.setConnectTimeoutMillis(connectTimeout);

        LOGGER.info("rpc client options set: {}", clientOption);
        return clientOption;
    }

    private static ChannelType getChannelType(Configuration config) {
        String channelType = config.getString(ExecutionConfigKeys.RPC_CHANNEL_CONNECT_TYPE);
        if (channelType.equalsIgnoreCase(SHORT_CONNECTION)) {
            return ChannelType.SHORT_CONNECTION;
        } else if (channelType.equalsIgnoreCase(SINGLE_CONNECTION)) {
            return ChannelType.SINGLE_CONNECTION;
        } else {
            return ChannelType.POOLED_CONNECTION;
        }
    }

}
