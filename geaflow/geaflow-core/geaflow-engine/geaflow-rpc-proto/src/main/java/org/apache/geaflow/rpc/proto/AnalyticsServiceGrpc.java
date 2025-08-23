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

package org.apache.geaflow.rpc.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 *
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.15.0)",
    comments = "Source: analytics.proto")
public class AnalyticsServiceGrpc {

    private AnalyticsServiceGrpc() {
    }

    public static final String SERVICE_NAME = "AnalyticsService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Analytics.QueryRequest,
        Analytics.QueryResult> METHOD_EXECUTE_QUERY =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "AnalyticsService", "executeQuery"),
            io.grpc.protobuf.ProtoUtils.marshaller(Analytics.QueryRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Analytics.QueryResult.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Analytics.QueryCancelRequest,
        Analytics.QueryCancelResult> METHOD_CANCEL_QUERY =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "AnalyticsService", "cancelQuery"),
            io.grpc.protobuf.ProtoUtils.marshaller(Analytics.QueryCancelRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Analytics.QueryCancelResult.getDefaultInstance()));

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static AnalyticsServiceStub newStub(io.grpc.Channel channel) {
        return new AnalyticsServiceStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static AnalyticsServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        return new AnalyticsServiceBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
     */
    public static AnalyticsServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        return new AnalyticsServiceFutureStub(channel);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface AnalyticsService {

        /**
         * <pre>
         * Execute query and return query result.
         * </pre>
         */
        public void executeQuery(Analytics.QueryRequest request,
                                 io.grpc.stub.StreamObserver<Analytics.QueryResult> responseObserver);

        /**
         * <pre>
         * Cancel query which is specified by queryId in QueryCancelRequest.
         * </pre>
         */
        public void cancelQuery(Analytics.QueryCancelRequest request,
                                io.grpc.stub.StreamObserver<Analytics.QueryCancelResult> responseObserver);
    }

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
    public static abstract class AnalyticsServiceImplBase implements AnalyticsService, io.grpc.BindableService {

        @java.lang.Override
        public void executeQuery(Analytics.QueryRequest request,
                                 io.grpc.stub.StreamObserver<Analytics.QueryResult> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_EXECUTE_QUERY, responseObserver);
        }

        @java.lang.Override
        public void cancelQuery(Analytics.QueryCancelRequest request,
                                io.grpc.stub.StreamObserver<Analytics.QueryCancelResult> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_CANCEL_QUERY, responseObserver);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return AnalyticsServiceGrpc.bindService(this);
        }
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface AnalyticsServiceBlockingClient {

        /**
         * <pre>
         * Execute query and return query result.
         * </pre>
         */
        public Analytics.QueryResult executeQuery(Analytics.QueryRequest request);

        /**
         * <pre>
         * Cancel query which is specified by queryId in QueryCancelRequest.
         * </pre>
         */
        public Analytics.QueryCancelResult cancelQuery(Analytics.QueryCancelRequest request);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface AnalyticsServiceFutureClient {

        /**
         * <pre>
         * Execute query and return query result.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<Analytics.QueryResult> executeQuery(
            Analytics.QueryRequest request);

        /**
         * <pre>
         * Cancel query which is specified by queryId in QueryCancelRequest.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<Analytics.QueryCancelResult> cancelQuery(
            Analytics.QueryCancelRequest request);
    }

    public static class AnalyticsServiceStub extends io.grpc.stub.AbstractStub<AnalyticsServiceStub>
        implements AnalyticsService {
        private AnalyticsServiceStub(io.grpc.Channel channel) {
            super(channel);
        }

        private AnalyticsServiceStub(io.grpc.Channel channel,
                                     io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected AnalyticsServiceStub build(io.grpc.Channel channel,
                                             io.grpc.CallOptions callOptions) {
            return new AnalyticsServiceStub(channel, callOptions);
        }

        @java.lang.Override
        public void executeQuery(Analytics.QueryRequest request,
                                 io.grpc.stub.StreamObserver<Analytics.QueryResult> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_EXECUTE_QUERY, getCallOptions()), request, responseObserver);
        }

        @java.lang.Override
        public void cancelQuery(Analytics.QueryCancelRequest request,
                                io.grpc.stub.StreamObserver<Analytics.QueryCancelResult> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_CANCEL_QUERY, getCallOptions()), request, responseObserver);
        }
    }

    public static class AnalyticsServiceBlockingStub extends io.grpc.stub.AbstractStub<AnalyticsServiceBlockingStub>
        implements AnalyticsServiceBlockingClient {
        private AnalyticsServiceBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private AnalyticsServiceBlockingStub(io.grpc.Channel channel,
                                             io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected AnalyticsServiceBlockingStub build(io.grpc.Channel channel,
                                                     io.grpc.CallOptions callOptions) {
            return new AnalyticsServiceBlockingStub(channel, callOptions);
        }

        @java.lang.Override
        public Analytics.QueryResult executeQuery(Analytics.QueryRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_EXECUTE_QUERY, getCallOptions(), request);
        }

        @java.lang.Override
        public Analytics.QueryCancelResult cancelQuery(Analytics.QueryCancelRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_CANCEL_QUERY, getCallOptions(), request);
        }
    }

    public static class AnalyticsServiceFutureStub extends io.grpc.stub.AbstractStub<AnalyticsServiceFutureStub>
        implements AnalyticsServiceFutureClient {
        private AnalyticsServiceFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private AnalyticsServiceFutureStub(io.grpc.Channel channel,
                                           io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected AnalyticsServiceFutureStub build(io.grpc.Channel channel,
                                                   io.grpc.CallOptions callOptions) {
            return new AnalyticsServiceFutureStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Analytics.QueryResult> executeQuery(
            Analytics.QueryRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_EXECUTE_QUERY, getCallOptions()), request);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Analytics.QueryCancelResult> cancelQuery(
            Analytics.QueryCancelRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_CANCEL_QUERY, getCallOptions()), request);
        }
    }

    @java.lang.Deprecated
    public static abstract class AbstractAnalyticsService extends AnalyticsServiceImplBase {
    }

    private static final int METHODID_EXECUTE_QUERY = 0;
    private static final int METHODID_CANCEL_QUERY = 1;

    private static class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final AnalyticsService serviceImpl;
        private final int methodId;

        public MethodHandlers(AnalyticsService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_EXECUTE_QUERY:
                    serviceImpl.executeQuery((Analytics.QueryRequest) request,
                        (io.grpc.stub.StreamObserver<Analytics.QueryResult>) responseObserver);
                    break;
                case METHODID_CANCEL_QUERY:
                    serviceImpl.cancelQuery((Analytics.QueryCancelRequest) request,
                        (io.grpc.stub.StreamObserver<Analytics.QueryCancelResult>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
            io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                default:
                    throw new AssertionError();
            }
        }
    }

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        return new io.grpc.ServiceDescriptor(SERVICE_NAME,
            METHOD_EXECUTE_QUERY,
            METHOD_CANCEL_QUERY);
    }

    @java.lang.Deprecated
    public static io.grpc.ServerServiceDefinition bindService(
        final AnalyticsService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
            .addMethod(
                METHOD_EXECUTE_QUERY,
                asyncUnaryCall(
                    new MethodHandlers<
                        Analytics.QueryRequest,
                        Analytics.QueryResult>(
                        serviceImpl, METHODID_EXECUTE_QUERY)))
            .addMethod(
                METHOD_CANCEL_QUERY,
                asyncUnaryCall(
                    new MethodHandlers<
                        Analytics.QueryCancelRequest,
                        Analytics.QueryCancelResult>(
                        serviceImpl, METHODID_CANCEL_QUERY)))
            .build();
    }
}
