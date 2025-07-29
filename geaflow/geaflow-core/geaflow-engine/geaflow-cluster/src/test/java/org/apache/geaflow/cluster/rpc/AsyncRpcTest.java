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

package org.apache.geaflow.cluster.rpc;

import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.DefaultRpcCallbackImpl;
import org.apache.geaflow.cluster.rpc.impl.RpcServiceImpl;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.ConfigurableServerOption;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.rpc.proto.Container;
import org.apache.geaflow.rpc.proto.Container.Request;
import org.apache.geaflow.rpc.proto.Container.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AsyncRpcTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRpcTest.class);

    @Test
    public void testAsyncRpc() throws Exception {

        Server server = new Server();
        server.startServer();
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 100;
        List<IEvent> request = new ArrayList<>();
        List<Future<IEvent>> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            IEvent event = new TestEvent(i);
            request.add(event);
            events.add(new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>())));
        }
        validateResult(events, eventCount, 5000);
        LOGGER.info("send event finish");
    }

    @Test
    public void testShutdownChannel() throws Exception {
        Server server = new Server();
        server.startServer();
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 1000;
        List<Integer> eventIds = new ArrayList<>();
        List<Integer> processedIds = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            event.processTimeMs = 1;
            Future<IEvent> future = new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>()));
            processedIds.add(((TestEvent) (future.get(1000, TimeUnit.MILLISECONDS))).id);
            eventIds.add(i);
            // Do shutdown.
            if (i % 100 == 0) {
                LOGGER.info("shutdown channel");
                server.stopServer();
                LOGGER.info("shutdown channel finish");
                SleepUtils.sleepMilliSecond(10);
            }
        }

        Assert.assertEquals(processedIds, eventIds);
        LOGGER.info("send event finish");
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testServerError() throws Exception {

        Server server = new Server();
        server.startServer();
        String host = ProcessUtil.getHostIp();
        ContainerEndpointRef client = new ContainerEndpointRef(host, server.rpcPort,
            new Configuration());

        int eventCount = 100;
        List<Future<IEvent>> results = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(i);
            if (i == 50) {
                event.isException = true;
            }
            results.add(new RpcResponseFuture(client.process(event, new DefaultRpcCallbackImpl<>())));
        }
        LOGGER.info("send event finish");
        validateResult(results, eventCount, 5000);
    }

    public void validateResult(List<Future<IEvent>> results, int count, int waitTimeMs)
        throws Exception {
        List<Integer> eventIds = new ArrayList<>();
        List<Integer> processedIds = new ArrayList<>();
        LOGGER.info("validate result");
        for (int i = 0; i < count; i++) {
            eventIds.add(i);
            processedIds.add(
                ((TestEvent) (results.get(i).get(waitTimeMs, TimeUnit.MILLISECONDS))).id);
        }
        Assert.assertEquals(processedIds, eventIds);
        LOGGER.info("validate finish");
    }

    /**
     * Mock event with dummy info.
     */
    public class TestEvent implements IEvent {

        private int id;
        private int processTimeMs;
        private boolean isException;

        public TestEvent(int id) {
            this.id = id;
        }


        @Override
        public EventType getEventType() {
            return null;
        }

        @Override
        public String toString() {
            return "TestEvent{" +
                "id=" + id +
                '}';
        }
    }

    public class Server {

        protected int rpcPort;

        protected Configuration configuration = new Configuration();
        protected RpcServiceImpl rpcService;

        public void startServer() {
            this.rpcService = new RpcServiceImpl(PortUtil.getPort(rpcPort),
                ConfigurableServerOption.build(configuration));
            this.rpcService.addEndpoint(new MockContainerEndpoint());
            this.rpcPort = rpcService.startService();
        }

        public void stopServer() {
            rpcService.stopService();
        }
    }

    /**
     * Mock endpoint to process events.
     */
    public class MockContainerEndpoint implements IContainerEndpoint {

        public MockContainerEndpoint() {
        }

        @Override
        public Response process(Request request) {
            try {
                IEvent event = RpcMessageEncoder.decode(request.getPayload());
                if (((TestEvent) event).processTimeMs > 0) {
                    SleepUtils.sleepMilliSecond(((TestEvent) event).processTimeMs);
                }
                if (((TestEvent) event).isException) {
                    LOGGER.info("on error: mock exception");
                    throw new GeaflowRuntimeException("occur mock exception");
                } else {
                    Container.Response.Builder builder =
                        Container.Response.newBuilder();
                    builder.setPayload(request.getPayload());
                    return builder.build();
                }
            } catch (Throwable t) {
                LOGGER.error("process request failed: {}", t.getMessage(), t);
                throw new GeaflowRuntimeException("process request failed", t);
            }
        }

        @Override
        public Empty close(Empty request) {
            try {
                LOGGER.info("close");
                return Empty.newBuilder().build();
            } catch (Throwable t) {
                LOGGER.error("close failed: {}", t.getMessage(), t);
                throw new GeaflowRuntimeException(String.format("close failed: %s", t.getMessage()), t);
            }
        }
    }

}
