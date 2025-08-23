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
    comments = "Source: metrics.proto")
public class MetricServiceGrpc {

    private MetricServiceGrpc() {
    }

    public static final String SERVICE_NAME = "MetricService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Metrics.MetricQueryRequest,
        Metrics.MetricQueryResponse> METHOD_QUERY_METRICS =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "MetricService", "queryMetrics"),
            io.grpc.protobuf.ProtoUtils.marshaller(Metrics.MetricQueryRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Metrics.MetricQueryResponse.getDefaultInstance()));

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static MetricServiceStub newStub(io.grpc.Channel channel) {
        return new MetricServiceStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static MetricServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        return new MetricServiceBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
     */
    public static MetricServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        return new MetricServiceFutureStub(channel);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface MetricService {

        /**
         *
         */
        public void queryMetrics(Metrics.MetricQueryRequest request,
                                 io.grpc.stub.StreamObserver<Metrics.MetricQueryResponse> responseObserver);
    }

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
    public static abstract class MetricServiceImplBase implements MetricService, io.grpc.BindableService {

        @java.lang.Override
        public void queryMetrics(Metrics.MetricQueryRequest request,
                                 io.grpc.stub.StreamObserver<Metrics.MetricQueryResponse> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_QUERY_METRICS, responseObserver);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return MetricServiceGrpc.bindService(this);
        }
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface MetricServiceBlockingClient {

        /**
         *
         */
        public Metrics.MetricQueryResponse queryMetrics(Metrics.MetricQueryRequest request);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface MetricServiceFutureClient {

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<Metrics.MetricQueryResponse> queryMetrics(
            Metrics.MetricQueryRequest request);
    }

    public static class MetricServiceStub extends io.grpc.stub.AbstractStub<MetricServiceStub>
        implements MetricService {
        private MetricServiceStub(io.grpc.Channel channel) {
            super(channel);
        }

        private MetricServiceStub(io.grpc.Channel channel,
                                  io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected MetricServiceStub build(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            return new MetricServiceStub(channel, callOptions);
        }

        @java.lang.Override
        public void queryMetrics(Metrics.MetricQueryRequest request,
                                 io.grpc.stub.StreamObserver<Metrics.MetricQueryResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_QUERY_METRICS, getCallOptions()), request, responseObserver);
        }
    }

    public static class MetricServiceBlockingStub extends io.grpc.stub.AbstractStub<MetricServiceBlockingStub>
        implements MetricServiceBlockingClient {
        private MetricServiceBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private MetricServiceBlockingStub(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected MetricServiceBlockingStub build(io.grpc.Channel channel,
                                                  io.grpc.CallOptions callOptions) {
            return new MetricServiceBlockingStub(channel, callOptions);
        }

        @java.lang.Override
        public Metrics.MetricQueryResponse queryMetrics(Metrics.MetricQueryRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_QUERY_METRICS, getCallOptions(), request);
        }
    }

    public static class MetricServiceFutureStub extends io.grpc.stub.AbstractStub<MetricServiceFutureStub>
        implements MetricServiceFutureClient {
        private MetricServiceFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private MetricServiceFutureStub(io.grpc.Channel channel,
                                        io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected MetricServiceFutureStub build(io.grpc.Channel channel,
                                                io.grpc.CallOptions callOptions) {
            return new MetricServiceFutureStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Metrics.MetricQueryResponse> queryMetrics(
            Metrics.MetricQueryRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_QUERY_METRICS, getCallOptions()), request);
        }
    }

    @java.lang.Deprecated
    public static abstract class AbstractMetricService extends MetricServiceImplBase {
    }

    private static final int METHODID_QUERY_METRICS = 0;

    private static class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final MetricService serviceImpl;
        private final int methodId;

        public MethodHandlers(MetricService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_QUERY_METRICS:
                    serviceImpl.queryMetrics((Metrics.MetricQueryRequest) request,
                        (io.grpc.stub.StreamObserver<Metrics.MetricQueryResponse>) responseObserver);
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
            METHOD_QUERY_METRICS);
    }

    @java.lang.Deprecated
    public static io.grpc.ServerServiceDefinition bindService(
        final MetricService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
            .addMethod(
                METHOD_QUERY_METRICS,
                asyncUnaryCall(
                    new MethodHandlers<
                        Metrics.MetricQueryRequest,
                        Metrics.MetricQueryResponse>(
                        serviceImpl, METHODID_QUERY_METRICS)))
            .build();
    }
}
