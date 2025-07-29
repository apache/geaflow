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
    comments = "Source: resource.proto")
public class ResourceServiceGrpc {

    private ResourceServiceGrpc() {
    }

    public static final String SERVICE_NAME = "ResourceService";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Resource.RequireResourceRequest,
        Resource.RequireResourceResponse> METHOD_REQUIRE_RESOURCE =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "ResourceService", "requireResource"),
            io.grpc.protobuf.ProtoUtils.marshaller(Resource.RequireResourceRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Resource.RequireResourceResponse.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<Resource.ReleaseResourceRequest,
        Resource.ReleaseResourceResponse> METHOD_RELEASE_RESOURCE =
        io.grpc.MethodDescriptor.create(
            io.grpc.MethodDescriptor.MethodType.UNARY,
            generateFullMethodName(
                "ResourceService", "releaseResource"),
            io.grpc.protobuf.ProtoUtils.marshaller(Resource.ReleaseResourceRequest.getDefaultInstance()),
            io.grpc.protobuf.ProtoUtils.marshaller(Resource.ReleaseResourceResponse.getDefaultInstance()));

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static ResourceServiceStub newStub(io.grpc.Channel channel) {
        return new ResourceServiceStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static ResourceServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        return new ResourceServiceBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
     */
    public static ResourceServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        return new ResourceServiceFutureStub(channel);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ResourceService {

        /**
         *
         */
        public void requireResource(Resource.RequireResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.RequireResourceResponse> responseObserver);

        /**
         *
         */
        public void releaseResource(Resource.ReleaseResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.ReleaseResourceResponse> responseObserver);
    }

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
    public static abstract class ResourceServiceImplBase implements ResourceService, io.grpc.BindableService {

        @java.lang.Override
        public void requireResource(Resource.RequireResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.RequireResourceResponse> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REQUIRE_RESOURCE, responseObserver);
        }

        @java.lang.Override
        public void releaseResource(Resource.ReleaseResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.ReleaseResourceResponse> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_RELEASE_RESOURCE, responseObserver);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return ResourceServiceGrpc.bindService(this);
        }
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ResourceServiceBlockingClient {

        /**
         *
         */
        public Resource.RequireResourceResponse requireResource(Resource.RequireResourceRequest request);

        /**
         *
         */
        public Resource.ReleaseResourceResponse releaseResource(Resource.ReleaseResourceRequest request);
    }

    /**
     *
     */
    @java.lang.Deprecated
    public static interface ResourceServiceFutureClient {

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<Resource.RequireResourceResponse> requireResource(
            Resource.RequireResourceRequest request);

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<Resource.ReleaseResourceResponse> releaseResource(
            Resource.ReleaseResourceRequest request);
    }

    public static class ResourceServiceStub extends io.grpc.stub.AbstractStub<ResourceServiceStub>
        implements ResourceService {
        private ResourceServiceStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ResourceServiceStub(io.grpc.Channel channel,
                                    io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ResourceServiceStub build(io.grpc.Channel channel,
                                            io.grpc.CallOptions callOptions) {
            return new ResourceServiceStub(channel, callOptions);
        }

        @java.lang.Override
        public void requireResource(Resource.RequireResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.RequireResourceResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_REQUIRE_RESOURCE, getCallOptions()), request, responseObserver);
        }

        @java.lang.Override
        public void releaseResource(Resource.ReleaseResourceRequest request,
                                    io.grpc.stub.StreamObserver<Resource.ReleaseResourceResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(METHOD_RELEASE_RESOURCE, getCallOptions()), request, responseObserver);
        }
    }

    public static class ResourceServiceBlockingStub extends io.grpc.stub.AbstractStub<ResourceServiceBlockingStub>
        implements ResourceServiceBlockingClient {
        private ResourceServiceBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ResourceServiceBlockingStub(io.grpc.Channel channel,
                                            io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ResourceServiceBlockingStub build(io.grpc.Channel channel,
                                                    io.grpc.CallOptions callOptions) {
            return new ResourceServiceBlockingStub(channel, callOptions);
        }

        @java.lang.Override
        public Resource.RequireResourceResponse requireResource(Resource.RequireResourceRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_REQUIRE_RESOURCE, getCallOptions(), request);
        }

        @java.lang.Override
        public Resource.ReleaseResourceResponse releaseResource(Resource.ReleaseResourceRequest request) {
            return blockingUnaryCall(
                getChannel(), METHOD_RELEASE_RESOURCE, getCallOptions(), request);
        }
    }

    public static class ResourceServiceFutureStub extends io.grpc.stub.AbstractStub<ResourceServiceFutureStub>
        implements ResourceServiceFutureClient {
        private ResourceServiceFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ResourceServiceFutureStub(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected ResourceServiceFutureStub build(io.grpc.Channel channel,
                                                  io.grpc.CallOptions callOptions) {
            return new ResourceServiceFutureStub(channel, callOptions);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Resource.RequireResourceResponse> requireResource(
            Resource.RequireResourceRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_REQUIRE_RESOURCE, getCallOptions()), request);
        }

        @java.lang.Override
        public com.google.common.util.concurrent.ListenableFuture<Resource.ReleaseResourceResponse> releaseResource(
            Resource.ReleaseResourceRequest request) {
            return futureUnaryCall(
                getChannel().newCall(METHOD_RELEASE_RESOURCE, getCallOptions()), request);
        }
    }

    @java.lang.Deprecated
    public static abstract class AbstractResourceService extends ResourceServiceImplBase {
    }

    private static final int METHODID_REQUIRE_RESOURCE = 0;
    private static final int METHODID_RELEASE_RESOURCE = 1;

    private static class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final ResourceService serviceImpl;
        private final int methodId;

        public MethodHandlers(ResourceService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_REQUIRE_RESOURCE:
                    serviceImpl.requireResource((Resource.RequireResourceRequest) request,
                        (io.grpc.stub.StreamObserver<Resource.RequireResourceResponse>) responseObserver);
                    break;
                case METHODID_RELEASE_RESOURCE:
                    serviceImpl.releaseResource((Resource.ReleaseResourceRequest) request,
                        (io.grpc.stub.StreamObserver<Resource.ReleaseResourceResponse>) responseObserver);
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
            METHOD_REQUIRE_RESOURCE,
            METHOD_RELEASE_RESOURCE);
    }

    @java.lang.Deprecated
    public static io.grpc.ServerServiceDefinition bindService(
        final ResourceService serviceImpl) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
            .addMethod(
                METHOD_REQUIRE_RESOURCE,
                asyncUnaryCall(
                    new MethodHandlers<
                        Resource.RequireResourceRequest,
                        Resource.RequireResourceResponse>(
                        serviceImpl, METHODID_REQUIRE_RESOURCE)))
            .addMethod(
                METHOD_RELEASE_RESOURCE,
                asyncUnaryCall(
                    new MethodHandlers<
                        Resource.ReleaseResourceRequest,
                        Resource.ReleaseResourceResponse>(
                        serviceImpl, METHODID_RELEASE_RESOURCE)))
            .build();
    }
}
