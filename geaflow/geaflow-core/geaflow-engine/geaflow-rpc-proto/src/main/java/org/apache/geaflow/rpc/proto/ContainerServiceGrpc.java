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
    comments = "Source: container.proto")
public class ContainerServiceGrpc {

    private ContainerServiceGrpc() {
    }

    public static final String SERVICE_NAME = "ContainerService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Container.Request,
        Container.Response> METHOD_PROCESS =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "ContainerService", "process"),
            io.grpc.protobuf.ProtoUtils.marshaller(Container.Request.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Container.Response.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.google.protobuf.Empty,
        com.google.protobuf.Empty> METHOD_CLOSE =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "ContainerService", "close"),
            io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

    /**
     * Creates a new async stub that supports all call types for the service.
     */
    public static ContainerServiceStub newStub(io.grpc.Channel channel) {
        return new ContainerServiceStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service.
     */
    public static ContainerServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        return new ContainerServiceBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service.
     */
    public static ContainerServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        return new ContainerServiceFutureStub(channel);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ContainerService {

        /**
         * <pre>
         * Messages from the master/driver are received and processed asynchronously.
         * </pre>
         */
        public void process(Container.Request request,
                            io.grpc.stub.StreamObserver<Container.Response> responseObserver);

        /**
         * <pre>
         * Close end container.
         * </pre>
         */
        public void close(com.google.protobuf.Empty request,
                          io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
    }

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
    public static abstract class ContainerServiceImplBase implements ContainerService, io.grpc.BindableService {

        @java.lang.Override
        public void process(Container.Request request,
                            io.grpc.stub.StreamObserver<Container.Response> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_PROCESS, responseObserver);
        }

        @java.lang.Override
        public void close(com.google.protobuf.Empty request,
                          io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_CLOSE, responseObserver);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return ContainerServiceGrpc.bindService(this);
        }
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ContainerServiceBlockingClient {

        /**
         * <pre>
         * Messages from the master/driver are received and processed asynchronously.
         * </pre>
         */
        public Container.Response process(Container.Request request);

        /**
         * <pre>
         * Close end container.
         * </pre>
         */
        public com.google.protobuf.Empty close(com.google.protobuf.Empty request);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ContainerServiceFutureClient {

        /**
         * <pre>
         * Messages from the master/driver are received and processed asynchronously.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<Container.Response> process(
            Container.Request request);

        /**
         * <pre>
         * Close end container.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> close(
            com.google.protobuf.Empty request);
    }

    public static class ContainerServiceStub extends io.grpc.stub.AbstractStub<ContainerServiceStub>
        implements ContainerService {
        private ContainerServiceStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ContainerServiceStub(io.grpc.Channel channel,
                                     io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ContainerServiceStub build(io.grpc.Channel channel,
                                             io.grpc.CallOptions callOptions) {
            return new ContainerServiceStub(channel, callOptions);
        }

        @java.lang.Override
        public void process(Container.Request request,
                            io.grpc.stub.StreamObserver<Container.Response> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_PROCESS, getCallOptions()), request, responseObserver);
        }

        @java.lang.Override
        public void close(com.google.protobuf.Empty request,
                          io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_CLOSE, getCallOptions()), request, responseObserver);
        }
    }

    public static class ContainerServiceBlockingStub extends io.grpc.stub.AbstractStub<ContainerServiceBlockingStub>
        implements ContainerServiceBlockingClient {
        private ContainerServiceBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ContainerServiceBlockingStub(io.grpc.Channel channel,
                                             io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ContainerServiceBlockingStub build(io.grpc.Channel channel,
                                                     io.grpc.CallOptions callOptions) {
            return new ContainerServiceBlockingStub(channel, callOptions);
        }

        @java.lang.Override
        public Container.Response process(Container.Request request) {
            return blockingUnaryCall(
                getChannel(), METHOD_PROCESS, getCallOptions(), request);
        }

        @java.lang.Override
        public com.google.protobuf.Empty close(com.google.protobuf.Empty request) {
            return blockingUnaryCall(
                getChannel(), METHOD_CLOSE, getCallOptions(), request);
        }
    }

    public static class ContainerServiceFutureStub extends io.grpc.stub.AbstractStub<ContainerServiceFutureStub>
        implements ContainerServiceFutureClient {
        private ContainerServiceFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ContainerServiceFutureStub(io.grpc.Channel channel,
                                           io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ContainerServiceFutureStub build(io.grpc.Channel channel,
                                                   io.grpc.CallOptions callOptions) {
            return new ContainerServiceFutureStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Container.Response> process(
            Container.Request request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_PROCESS, getCallOptions()), request);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> close(
            com.google.protobuf.Empty request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_CLOSE, getCallOptions()), request);
        }
    }

    @java.lang.Deprecated
    public static abstract class AbstractContainerService extends ContainerServiceImplBase {
    }

    private static final int METHODID_PROCESS = 0;
    private static final int METHODID_CLOSE = 1;

    private static class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final ContainerService serviceImpl;
        private final int methodId;

        public MethodHandlers(ContainerService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_PROCESS:
                    serviceImpl.process((Container.Request) request,
                        (io.grpc.stub.StreamObserver<Container.Response>) responseObserver);
                    break;
                case METHODID_CLOSE:
                    serviceImpl.close((com.google.protobuf.Empty) request,
                        (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
            METHOD_PROCESS,
            METHOD_CLOSE);
    }

    @java.lang.Deprecated
    public static io.grpc.ServerServiceDefinition bindService(
        final ContainerService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
            .addMethod(
                METHOD_PROCESS,
                asyncUnaryCall(
                    new MethodHandlers<
                        Container.Request,
                        Container.Response>(
                        serviceImpl, METHODID_PROCESS)))
            .addMethod(
                METHOD_CLOSE,
                asyncUnaryCall(
                    new MethodHandlers<
                        com.google.protobuf.Empty,
                        com.google.protobuf.Empty>(
                        serviceImpl, METHODID_CLOSE)))
            .build();
    }
}
