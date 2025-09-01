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

package org.apache.geaflow.cluster.container;

import com.baidu.brpc.server.RpcServerOptions;
import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.cluster.collector.EmitterService;
import org.apache.geaflow.cluster.common.AbstractContainer;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.fetcher.FetcherService;
import org.apache.geaflow.cluster.protocol.ICommand;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.protocol.OpenContainerEvent;
import org.apache.geaflow.cluster.protocol.OpenContainerResponseEvent;
import org.apache.geaflow.cluster.rpc.impl.ContainerEndpoint;
import org.apache.geaflow.cluster.rpc.impl.RpcServiceImpl;
import org.apache.geaflow.cluster.task.service.TaskService;
import org.apache.geaflow.cluster.worker.Dispatcher;
import org.apache.geaflow.cluster.worker.DispatcherService;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.ConfigurableServerOption;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Container extends AbstractContainer implements IContainer<IEvent, IEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

    private ContainerContext containerContext;
    private Dispatcher dispatcher;
    private AtomicBoolean isOpened;
    protected FetcherService fetcherService;
    protected EmitterService emitterService;
    protected TaskService workerService;
    protected DispatcherService dispatcherService;

    public Container() {
        this(0);
    }

    public Container(int rpcPort) {
        super(rpcPort);
        this.isOpened = new AtomicBoolean(false);
    }

    @Override
    public void init(ContainerContext containerContext) {
        try {
            this.containerContext = containerContext;
            String containerName = ClusterConstants.getContainerName(containerContext.getId());
            super.init(containerContext.getId(), containerName, containerContext.getConfig());
            registerToMaster();
            LOGGER.info("container {} init finish", name);
        } catch (Throwable t) {
            LOGGER.error("init container err", t);
            throw new GeaflowRuntimeException(t);
        }
    }

    @Override
    protected void startRpcService() {
        RpcServerOptions serverOptions = ConfigurableServerOption.build(configuration);
        this.rpcService = new RpcServiceImpl(PortUtil.getPort(rpcPort), serverOptions);
        this.rpcService.addEndpoint(new ContainerEndpoint(this));
        this.rpcPort = rpcService.startService();
    }

    public OpenContainerResponseEvent open(OpenContainerEvent event) {
        try {
            if (isOpened.compareAndSet(false, true)) {
                int num = event.getExecutorNum();
                Preconditions.checkArgument(num > 0, "worker num should > 0");
                LOGGER.info("open container {} with {} executors", name, num);

                this.fetcherService = new FetcherService(num, configuration);
                this.emitterService = new EmitterService(num, configuration);
                this.workerService = new TaskService(id, num,
                    configuration, metricGroup, fetcherService, emitterService);
                this.dispatcher = new Dispatcher(workerService);
                this.dispatcherService = new DispatcherService(dispatcher, configuration);

                // start task service
                this.fetcherService.start();
                this.emitterService.start();
                this.workerService.start();
                this.dispatcherService.start();

                if (containerContext.getReliableEvents() != null) {
                    for (IEvent reliableEvent : containerContext.getReliableEvents()) {
                        LOGGER.info("{} replay event {}", name, reliableEvent);
                        this.dispatcher.add((ICommand) reliableEvent);
                    }
                }
                registerHAService();
            }
            return new OpenContainerResponseEvent(id, 0);
        } catch (Throwable throwable) {
            LOGGER.error("{} open error", name, throwable);
            throw throwable;
        }
    }

    @Override
    public IEvent process(IEvent input) {
        LOGGER.info("{} process event {}", name, input);
        try {
            this.containerContext.addEvent(input);
            this.containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());
            this.dispatcher.add((ICommand) input);
            return null;
        } catch (Throwable throwable) {
            LOGGER.error("{} process error", name, throwable);
            throw throwable;
        }
    }

    @Override
    public void close() {
        super.close();
        if (fetcherService != null) {
            fetcherService.shutdown();
        }
        if (workerService != null) {
            workerService.shutdown();
        }
        if (dispatcherService != null) {
            dispatcherService.shutdown();
        }
        if (emitterService != null) {
            emitterService.shutdown();
        }
        LOGGER.info("container {} closed", name);
    }

    @Override
    protected ContainerInfo buildComponentInfo() {
        ContainerInfo containerInfo = new ContainerInfo();
        fillComponentInfo(containerInfo);
        containerInfo.setShufflePort(ShuffleManager.getInstance().getShufflePort());
        return containerInfo;
    }
}
