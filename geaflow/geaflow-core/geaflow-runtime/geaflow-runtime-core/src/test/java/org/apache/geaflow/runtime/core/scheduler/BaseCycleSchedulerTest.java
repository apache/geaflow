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

package org.apache.geaflow.runtime.core.scheduler;

import static org.mockito.ArgumentMatchers.any;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.geaflow.cluster.common.IEventListener;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.resourcemanager.RequireResourceRequest;
import org.apache.geaflow.cluster.resourcemanager.RequireResponse;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef;
import org.apache.geaflow.rpc.proto.Container;
import org.apache.geaflow.runtime.core.protocol.AbstractExecutableCommand;
import org.apache.geaflow.runtime.core.protocol.ComposeEvent;
import org.apache.geaflow.runtime.core.protocol.DoneEvent;
import org.apache.geaflow.runtime.core.protocol.LaunchSourceEvent;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseCycleSchedulerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCycleSchedulerTest.class);

    private MockedStatic<RpcClient> rpcClientMs;
    protected MockContainerEventProcessor processor;

    @BeforeMethod
    public void beforeMethod() {
        processor = new MockContainerEventProcessor();
        // mock resource manager rpc
        RpcClient rpcClient = Mockito.mock(RpcClient.class);
        rpcClientMs = Mockito.mockStatic(RpcClient.class);
        rpcClientMs.when(() -> RpcClient.getInstance()).then(invocation -> rpcClient);

        Mockito.doAnswer(in -> {
            int workerNum = ((RequireResourceRequest) in.getArgument(1)).getRequiredNum();
            List<WorkerInfo> workers = new ArrayList<>();
            for (int i = 0; i < workerNum; i++) {
                WorkerInfo workerInfo = new WorkerInfo();
                workerInfo.setHost("host0");
                workerInfo.setContainerName("container0");
                workerInfo.setWorkerIndex(i);
                workers.add(workerInfo);
            }
            return RequireResponse.success("test", workers);
        }).when(rpcClient).requireResource(any(), any());

        // mock container rpc
        Mockito.doAnswer(in -> {
            processor.process((IEvent) in.getArgument(1));
            CompletableFuture future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }).when(rpcClient).processContainer(any(), any());

        Mockito.doAnswer(in -> {
            processor.process((IEvent) in.getArgument(1));
            RpcEndpointRef.RpcCallback<Container.Response> callback = ((RpcEndpointRef.RpcCallback) in.getArgument(2));
            callback.onSuccess(null);
            return null;
        }).when(rpcClient).processContainer(any(), any(), any());
    }

    @AfterMethod
    public void afterMethod() {
        rpcClientMs.close();
        processor.clean();
    }

    public class MockContainerEventProcessor {

        private List<IEvent> processed;
        private ICycleScheduler scheduler;

        public MockContainerEventProcessor() {
            this.processed = new ArrayList<>();
        }

        public void register(ICycleScheduler scheduler) {
            this.scheduler = scheduler;
        }

        public void clean() {
            processed.clear();
            this.scheduler = null;
        }

        public void process(IEvent event) {
            LOGGER.info("process event {}", event);
            processed.add(event);
            processInternal(event);
        }

        public void processInternal(IEvent event) {
            if (event.getEventType() == EventType.COMPOSE) {
                for (IEvent e : ((ComposeEvent) event).getEventList()) {
                    processInternal(e);
                }
            } else {
                IEvent response;
                switch (event.getEventType()) {
                    case LAUNCH_SOURCE:
                        LaunchSourceEvent sourceEvent = (LaunchSourceEvent) event;
                        response = new DoneEvent<>(sourceEvent.getSchedulerId(), sourceEvent.getCycleId(), sourceEvent.getIterationWindowId(),
                            sourceEvent.getWorkerId(), EventType.EXECUTE_COMPUTE);
                        ((IEventListener) scheduler).handleEvent(response);
                        break;
                    case CLEAN_CYCLE:
                    case CLEAN_ENV:
                    case STASH_WORKER:
                        AbstractExecutableCommand executableCommand = (AbstractExecutableCommand) event;
                        response = new DoneEvent<>(executableCommand.getSchedulerId(), executableCommand.getCycleId(), executableCommand.getIterationWindowId(),
                            executableCommand.getWorkerId(), executableCommand.getEventType());
                        ((IEventListener) scheduler).handleEvent(response);
                        break;
                    default:

                }
            }
        }

        public List<IEvent> getProcessed() {
            return processed;
        }
    }
}
