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

package org.apache.geaflow.analytics.service.client;

import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.ABORTED;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.ERROR;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.FINISHED;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.RUNNING;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_NO_COORDINATOR;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_SERVER_BUSY;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.analytics.service.query.StandardError;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.LogMsgUtil;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.apache.geaflow.rpc.proto.Analytics;
import org.apache.geaflow.rpc.proto.Analytics.QueryRequest;
import org.apache.geaflow.rpc.proto.Analytics.QueryResult;
import org.apache.geaflow.rpc.proto.AnalyticsServiceGrpc;
import org.apache.geaflow.rpc.proto.AnalyticsServiceGrpc.AnalyticsServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcQueryRunner extends AbstractQueryRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcQueryRunner.class);
    private static final int DEFAULT_SHUTDOWN_AWAIT_MS = 5000;
    private static final ISerializer SERIALIZER = SerializerFactory.getKryoSerializer();
    private static final String REQUEST = "request";
    private Map<HostAndPort, ManagedChannel> coordinatorAddress2Channel;
    private Map<HostAndPort, AnalyticsServiceBlockingStub> coordinatorAddress2Stub;

    @Override
    public void init(QueryRunnerContext context) {
        this.coordinatorAddress2Channel = new HashMap<>();
        this.coordinatorAddress2Stub = new HashMap<>();
        super.init(context);
    }

    @Override
    public QueryResults executeQuery(String queryScript) {
        int coordinatorNum = analyticsServiceInfo.getCoordinatorNum();
        if (coordinatorNum == 0) {
            QueryError queryError = ANALYTICS_NO_COORDINATOR.getQueryError();
            return new QueryResults(queryError);
        }
        int idx = RANDOM.nextInt(coordinatorNum);
        List<HostAndPort> coordinatorAddresses = analyticsServiceInfo.getCoordinatorAddresses();
        QueryResults result = null;
        for (int i = 0; i < coordinatorAddresses.size(); i++) {
            HostAndPort address = coordinatorAddresses.get(idx);
            final long start = System.currentTimeMillis();
            result = executeInternal(address, queryScript);
            LOGGER.info("coordinator {} execute query script {} finish, cost {} ms", address, queryScript, System.currentTimeMillis() - start);
            if (!result.getQueryStatus() && result.getError().getCode() == ANALYTICS_SERVER_BUSY.getQueryError().getCode()) {
                LOGGER.warn("coordinator[{}] [{}] is busy, try next", idx, address.toString());
                idx = (idx + 1) % coordinatorNum;
                continue;
            }
            queryRunnerStatus.compareAndSet(RUNNING, FINISHED);
            return result;
        }

        if (result != null && (!result.getQueryStatus() && result.getError().getCode() == ANALYTICS_SERVER_BUSY.getQueryError().getCode())) {
            QueryError queryError = ANALYTICS_SERVER_BUSY.getQueryError();
            LOGGER.error(queryError.getName());
            queryRunnerStatus.compareAndSet(RUNNING, ERROR);
            return new QueryResults(queryError);
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.analyticsClientError(String.format("execute query [%s] error", queryScript)));
    }

    @Override
    public void initManagedChannel(HostAndPort address) {
        ManagedChannel coordinatorChannel = this.coordinatorAddress2Channel.get(address);
        if (coordinatorChannel != null && (!coordinatorChannel.isShutdown() || !coordinatorChannel.isTerminated())) {
            coordinatorChannel.shutdownNow();
            this.coordinatorAddress2Channel.remove(address);
        }
        ManagedChannel managedChannel = createManagedChannel(config, address);
        this.coordinatorAddress2Channel.put(address, managedChannel);
        this.coordinatorAddress2Stub.put(address, AnalyticsServiceGrpc.newBlockingStub(managedChannel));
    }

    public void ensureChannelAlive(HostAndPort address) {
        ManagedChannel channel = this.coordinatorAddress2Channel.get(address);
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            LOGGER.warn("connection of [{}:{}] lost, reconnect...", address.getHost(), address.getPort());
            this.initManagedChannel(address);
        }
    }

    private ManagedChannel buildChannel(String host, int port, int timeoutMs) {
        return NettyChannelBuilder.forAddress(host, port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
            .maxInboundMessageSize(config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_MAX_INBOUND_MESSAGE_SIZE))
            .maxRetryAttempts(config.getInteger(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_MAX_RETRY_ATTEMPTS))
            .retryBufferSize(config.getLong(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_DEFALUT_RETRY_BUFFER_SIZE))
            .perRpcBufferLimit(config.getLong(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_PER_RPC_BUFFER_LIMIT))
            .usePlaintext()
            .build();
    }


    private QueryResults executeInternal(HostAndPort address, String script) {
        AnalyticsServiceBlockingStub analyticsServiceBlockingStub = getAnalyticsServiceBlockingStub(address);
        if (analyticsServiceBlockingStub == null) {
            initManagedChannel(address);
            analyticsServiceBlockingStub = getAnalyticsServiceBlockingStub(address);
        }
        QueryRequest queryRequest = buildRpcRequest(script);
        QueryResult queryResponse;
        try {
            queryResponse = analyticsServiceBlockingStub.executeQuery(queryRequest);
        } catch (StatusRuntimeException e) {
            LOGGER.error("query {} execute failed with status {} and cause {}", script, e.getStatus(), LogMsgUtil.getStackMsg(e));
            return getResultIfException(e, address);
        }
        return RpcMessageEncoder.decode(queryResponse.getQueryResult());
    }

    public AnalyticsServiceBlockingStub getAnalyticsServiceBlockingStub(HostAndPort address) {
        ensureChannelAlive(address);
        AnalyticsServiceBlockingStub analyticsServiceBlockingStub = this.coordinatorAddress2Stub.get(address);
        if (analyticsServiceBlockingStub != null) {
            return analyticsServiceBlockingStub;
        }
        throw new GeaflowRuntimeException(String.format("coordinator address [%s:%d] get rpc stub "
            + "fail", address.getHost(), address.getPort()));
    }


    private QueryResults getResultIfException(StatusRuntimeException e, HostAndPort address) {
        QueryError queryError;
        switch (e.getStatus().getCode()) {
            case UNAVAILABLE:
                LOGGER.warn("coordinator address {} server unavailable", address);
                initAnalyticsServiceAddress();
                queryError = StandardError.ANALYTICS_SERVER_UNAVAILABLE.getQueryError();
                queryRunnerStatus.compareAndSet(RUNNING, ABORTED);
                return new QueryResults(queryError);
            case RESOURCE_EXHAUSTED:
                LOGGER.warn("query result too big: {}", e.getMessage());
                queryRunnerStatus.compareAndSet(RUNNING, ERROR);
                queryError = StandardError.ANALYTICS_RESULT_TO_LONG.getQueryError();
                return new QueryResults(queryError);
            default:
                LOGGER.warn("re-init channel {} for unexpected exception: {}", address, e.getStatus());
                queryRunnerStatus.compareAndSet(RUNNING, ERROR);
                initManagedChannel(address);
                queryError = StandardError.ANALYTICS_RPC_ERROR.getQueryError();
                return new QueryResults(queryError);
        }
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_rpc;
    }

    @Override
    public QueryResults cancelQuery(long queryId) {
        throw new GeaflowRuntimeException("not support cancel query");
    }

    @Override
    public void close() throws IOException {
        for (Entry<HostAndPort, ManagedChannel> entry : this.coordinatorAddress2Channel.entrySet()) {
            ManagedChannel channel = entry.getValue();
            HostAndPort address = entry.getKey();
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(DEFAULT_SHUTDOWN_AWAIT_MS, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LOGGER.warn("coordinator [{}:{}] shutdown failed", address.getHost(), address.getPort(), e);
                    throw new GeaflowRuntimeException(String.format("coordinator [%s:%d] "
                        + "shutdown error", address.getHost(), address.getPort()), e);
                }
            }
        }
        if (this.serverQueryClient != null) {
            this.serverQueryClient.close();
        }

        LOGGER.info("rpc query executor shutdown");
    }

    private ManagedChannel createManagedChannel(Configuration config, HostAndPort address) {
        Throwable latestException = null;
        int connectTimeoutMs = config.getInteger(ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS);
        int retryNum = config.getInteger(ANALYTICS_CLIENT_CONNECT_RETRY_NUM);
        for (int i = 0; i < retryNum; i++) {
            try {
                ManagedChannel managedChannel = buildChannel(address.getHost(), address.getPort(), connectTimeoutMs);
                LOGGER.info("init managed channel with address {}:{}", address.getHost(), address.getPort());
                return managedChannel;
            } catch (Throwable e) {
                latestException = e;
                LOGGER.warn("init managed channel [{}:{}] failed, retry {}", address.getHost(), address.getPort(), i + 1, e);
            }
        }
        String msg = String.format("try connect to [%s:%d] fail after %d times", address.getHost(), address.getPort(), retryNum);
        LOGGER.error(msg, latestException);
        throw new GeaflowRuntimeException(RuntimeErrors.INST.analyticsClientError(msg), latestException);
    }

    private Analytics.QueryRequest buildRpcRequest(String queryScript) {
        ByteString.Output output = ByteString.newOutput();
        SERIALIZER.serialize(REQUEST, output);
        return Analytics.QueryRequest.newBuilder()
            .setQuery(queryScript)
            .setQueryConfig(output.toByteString())
            .build();
    }

}
