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

package org.apache.geaflow.analytics.service.server.rpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryIdGenerator;
import org.apache.geaflow.analytics.service.query.QueryInfo;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.analytics.service.query.StandardError;
import org.apache.geaflow.analytics.service.server.AbstractAnalyticsServiceServer;
import org.apache.geaflow.common.blocking.map.BlockingMap;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.apache.geaflow.rpc.proto.Analytics;
import org.apache.geaflow.rpc.proto.Analytics.QueryCancelRequest;
import org.apache.geaflow.rpc.proto.Analytics.QueryCancelResult;
import org.apache.geaflow.rpc.proto.Analytics.QueryResult;
import org.apache.geaflow.rpc.proto.AnalyticsServiceGrpc;
import org.apache.geaflow.runtime.core.scheduler.result.IExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcAnalyticsServiceServer extends AbstractAnalyticsServiceServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcAnalyticsServiceServer.class);

    private Server server;

    @Override
    public void startServer() {
        try {
            // Will support server ip and port report in future.
            this.server = ServerBuilder.forPort(this.port).addService(new CoordinatorImpl(this)).build().start();
            String hostIpAddress = ProcessUtil.getHostIp();
            this.port = this.server.getPort();
            LOGGER.info("Server started: {}, listening on: {}", hostIpAddress, port);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                LOGGER.warn("*** Shutting down analytics server since JVM is shutting down.");
                stopServer();
                LOGGER.warn("*** Geaflow analytics server shutdown.");
            }));
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw new GeaflowRuntimeException(t);
        }
        waitForExecuted();
    }

    @Override
    public void stopServer() {
        super.stopServer();
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_rpc;
    }

    static class CoordinatorImpl extends AnalyticsServiceGrpc.AnalyticsServiceImplBase {

        private final BlockingQueue<QueryInfo> requestBlockingQueue;
        private final BlockingMap<String, Future<IExecutionResult>> responseBlockingMap;
        private final BlockingQueue<Long> cancelRequestBlockingQueue;
        private final BlockingQueue<Object> cancelResponseBlockingQueue;
        private final QueryIdGenerator queryIdGenerator;
        private final Semaphore semaphore;

        public CoordinatorImpl(RpcAnalyticsServiceServer server) {
            this.requestBlockingQueue = server.requestBlockingQueue;
            this.responseBlockingMap = server.responseBlockingMap;
            this.cancelRequestBlockingQueue = server.cancelRequestBlockingQueue;
            this.cancelResponseBlockingQueue = server.cancelResponseBlockingQueue;
            this.semaphore = server.semaphore;
            this.queryIdGenerator = new QueryIdGenerator();
        }

        @Override
        public void executeQuery(Analytics.QueryRequest request, StreamObserver<QueryResult> responseObserver) {
            String queryId = queryIdGenerator.createQueryId();
            if (!this.semaphore.tryAcquire()) {
                QueryError queryError = StandardError.ANALYTICS_SERVER_BUSY.getQueryError();
                QueryResults queryResults = new QueryResults(queryId, queryError);
                QueryResult result = QueryResult.newBuilder()
                    .setQueryResult(RpcMessageEncoder.encode(queryResults))
                    .build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }
            try {
                String query = request.getQuery();
                QueryInfo queryInfo = new QueryInfo(queryId, query);
                final long start = System.currentTimeMillis();
                requestBlockingQueue.put(queryInfo);
                QueryResults queryResults = getQueryResults(queryInfo, responseBlockingMap);
                LOGGER.info("finish execute query [{}], cost {}ms, query result {}", queryInfo,
                    System.currentTimeMillis() - start, queryResults);
                QueryResult queryResult = QueryResult.newBuilder()
                    .setQueryResult(RpcMessageEncoder.encode(queryResults))
                    .build();
                responseObserver.onNext(queryResult);
                responseObserver.onCompleted();
            } catch (Throwable t) {
                LOGGER.error("execute query: [{}] failed, cause: {}", request.getQuery(), t);
                QueryResults queryResults = new QueryResults(queryId, new QueryError(t.getMessage()));
                QueryResult result = QueryResult.newBuilder()
                    .setQueryResult(RpcMessageEncoder.encode(queryResults))
                    .build();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } finally {
                this.semaphore.release();
            }
        }

        @Override
        public void cancelQuery(QueryCancelRequest request, StreamObserver<QueryCancelResult> responseObserver) {
            throw new GeaflowRuntimeException("Not supported cancel query yet.");
        }
    }
}
