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
    comments = "Source: supervisor.proto")
public class SupervisorServiceGrpc {

    private SupervisorServiceGrpc() {
    }

    public static final String SERVICE_NAME = "SupervisorService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Supervisor.RestartRequest,
        com.google.protobuf.Empty> METHOD_RESTART =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "SupervisorService", "restart"),
            io.grpc.protobuf.ProtoUtils.marshaller(Supervisor.RestartRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.google.protobuf.Empty,
        Supervisor.StatusResponse> METHOD_STATUS =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "SupervisorService", "status"),
            io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Supervisor.StatusResponse.getDefaultInstance()));

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static SupervisorServiceStub newStub(io.grpc.Channel channel) {
        return new SupervisorServiceStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static SupervisorServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        return new SupervisorServiceBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
     */
    public static SupervisorServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        return new SupervisorServiceFutureStub(channel);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface SupervisorService {

        /**
         * <pre>
         * 启动container
         * </pre>
         */
        public void restart(Supervisor.RestartRequest request,
                            io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

        /**
         * <pre>
         * check status
         * </pre>
         */
        public void status(com.google.protobuf.Empty request,
                           io.grpc.stub.StreamObserver<Supervisor.StatusResponse> responseObserver);
    }

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
    public static abstract class SupervisorServiceImplBase implements SupervisorService, io.grpc.BindableService {

        @java.lang.Override
        public void restart(Supervisor.RestartRequest request,
                            io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_RESTART, responseObserver);
        }

        @java.lang.Override
        public void status(com.google.protobuf.Empty request,
                           io.grpc.stub.StreamObserver<Supervisor.StatusResponse> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_STATUS, responseObserver);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return SupervisorServiceGrpc.bindService(this);
        }
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface SupervisorServiceBlockingClient {

        /**
         * <pre>
         * 启动container
         * </pre>
         */
        public com.google.protobuf.Empty restart(Supervisor.RestartRequest request);

        /**
         * <pre>
         * check status
         * </pre>
         */
        public Supervisor.StatusResponse status(com.google.protobuf.Empty request);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface SupervisorServiceFutureClient {

        /**
         * <pre>
         * 启动container
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> restart(
            Supervisor.RestartRequest request);

        /**
         * <pre>
         * check status
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<Supervisor.StatusResponse> status(
            com.google.protobuf.Empty request);
    }

    public static class SupervisorServiceStub extends io.grpc.stub.AbstractStub<SupervisorServiceStub>
        implements SupervisorService {
        private SupervisorServiceStub(io.grpc.Channel channel) {
            super(channel);
        }

        private SupervisorServiceStub(io.grpc.Channel channel,
                                      io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected SupervisorServiceStub build(io.grpc.Channel channel,
                                              io.grpc.CallOptions callOptions) {
            return new SupervisorServiceStub(channel, callOptions);
        }

        @java.lang.Override
        public void restart(Supervisor.RestartRequest request,
                            io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_RESTART, getCallOptions()), request, responseObserver);
        }

        @java.lang.Override
        public void status(com.google.protobuf.Empty request,
                           io.grpc.stub.StreamObserver<Supervisor.StatusResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_STATUS, getCallOptions()), request, responseObserver);
        }
    }

    public static class SupervisorServiceBlockingStub extends io.grpc.stub.AbstractStub<SupervisorServiceBlockingStub>
        implements SupervisorServiceBlockingClient {
        private SupervisorServiceBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private SupervisorServiceBlockingStub(io.grpc.Channel channel,
                                              io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected SupervisorServiceBlockingStub build(io.grpc.Channel channel,
                                                      io.grpc.CallOptions callOptions) {
            return new SupervisorServiceBlockingStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.protobuf.Empty restart(Supervisor.RestartRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_RESTART, getCallOptions(), request);
        }

        @java.lang.Override
        public Supervisor.StatusResponse status(com.google.protobuf.Empty request) {
            return blockingUnaryCall(
                getChannel(), METHOD_STATUS, getCallOptions(), request);
        }
    }

    public static class SupervisorServiceFutureStub extends io.grpc.stub.AbstractStub<SupervisorServiceFutureStub>
        implements SupervisorServiceFutureClient {
        private SupervisorServiceFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private SupervisorServiceFutureStub(io.grpc.Channel channel,
                                            io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected SupervisorServiceFutureStub build(io.grpc.Channel channel,
                                                    io.grpc.CallOptions callOptions) {
            return new SupervisorServiceFutureStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> restart(
            Supervisor.RestartRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_RESTART, getCallOptions()), request);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Supervisor.StatusResponse> status(
            com.google.protobuf.Empty request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_STATUS, getCallOptions()), request);
        }
    }

    @java.lang.Deprecated
    public static abstract class AbstractSupervisorService extends SupervisorServiceImplBase {
    }

    private static final int METHODID_RESTART = 0;
    private static final int METHODID_STATUS = 1;

    private static class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final SupervisorService serviceImpl;
        private final int methodId;

        public MethodHandlers(SupervisorService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_RESTART:
                    serviceImpl.restart((Supervisor.RestartRequest) request,
                        (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
                    break;
                case METHODID_STATUS:
                    serviceImpl.status((com.google.protobuf.Empty) request,
                        (io.grpc.stub.StreamObserver<Supervisor.StatusResponse>) responseObserver);
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
            METHOD_RESTART,
            METHOD_STATUS);
    }

    @java.lang.Deprecated
    public static io.grpc.ServerServiceDefinition bindService(
        final SupervisorService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
            .addMethod(
                METHOD_RESTART,
                asyncUnaryCall(
                    new MethodHandlers<
                        Supervisor.RestartRequest,
                        com.google.protobuf.Empty>(
                        serviceImpl, METHODID_RESTART)))
            .addMethod(
                METHOD_STATUS,
                asyncUnaryCall(
                    new MethodHandlers<
                        com.google.protobuf.Empty,
                        Supervisor.StatusResponse>(
                        serviceImpl, METHODID_STATUS)))
            .build();
    }
}
