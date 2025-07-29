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

import static org.apache.geaflow.metaserver.service.NamespaceType.DEFAULT;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.metaserver.client.MetaServerQueryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQueryRunner implements IQueryRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueryRunner.class);

    protected static final Random RANDOM = new Random(System.currentTimeMillis());
    protected List<HostAndPort> coordinatorAddresses;

    protected Configuration config;
    protected MetaServerQueryClient serverQueryClient;
    protected AnalyticsServiceInfo analyticsServiceInfo;
    protected HostAndPort hostAndPort;
    protected AtomicReference<QueryRunnerStatus> queryRunnerStatus;
    private QueryRunnerContext context;

    @Override
    public void init(QueryRunnerContext context) {
        this.context = context;
        this.config = context.getConfiguration();
        this.hostAndPort = context.getHostAndPort();
        initAnalyticsServiceAddress();
        initManagedChannel();
        this.queryRunnerStatus = new AtomicReference<>(QueryRunnerStatus.RUNNING);
    }

    protected void initManagedChannel() {
        for (HostAndPort coordinatorAddress : this.coordinatorAddresses) {
            initManagedChannel(coordinatorAddress);
        }
    }

    protected abstract void initManagedChannel(HostAndPort address);

    public void initAnalyticsServiceAddress() {
        if (hostAndPort != null) {
            String serverName = String.format("%s:%d", hostAndPort.getHost(), hostAndPort.getPort());
            List<HostAndPort> hostAndPorts = Collections.singletonList(hostAndPort);
            this.analyticsServiceInfo = new AnalyticsServiceInfo(hostAndPorts);
            this.coordinatorAddresses = hostAndPorts;
            LOGGER.info("init single analytics service: [{}] finish", serverName);
            return;
        }
        this.serverQueryClient = MetaServerQueryClient.getClient(config);
        List<HostAndPort> serviceAddresses = getServiceAddresses();
        this.coordinatorAddresses = serviceAddresses;
        this.analyticsServiceInfo = new AnalyticsServiceInfo(serviceAddresses);
        LOGGER.info("init analytics service finish by meta server");
    }

    private List<HostAndPort> getServiceAddresses() {
        List<HostAndPort> hostAndPorts;
        try {
            hostAndPorts = serverQueryClient.queryAllServices(DEFAULT);
        } catch (Throwable e) {
            throw new GeaflowRuntimeException("query analytics coordinator addresses failed", e);
        }
        if (CollectionUtils.isEmpty(hostAndPorts)) {
            throw new GeaflowRuntimeException("query analytics coordinator addresses is empty");
        }
        LOGGER.info("query analytics coordinator addresses is {}", Arrays.toString(hostAndPorts.toArray()));
        return hostAndPorts;
    }

    @Override
    public boolean isRunning() {
        return queryRunnerStatus.get() == QueryRunnerStatus.RUNNING;
    }

    @Override
    public boolean isAborted() {
        return queryRunnerStatus.get() == QueryRunnerStatus.ABORTED;
    }

    @Override
    public boolean isError() {
        return queryRunnerStatus.get() == QueryRunnerStatus.ERROR;
    }

    @Override
    public boolean isFinished() {
        return queryRunnerStatus.get() == QueryRunnerStatus.FINISHED;
    }

}
