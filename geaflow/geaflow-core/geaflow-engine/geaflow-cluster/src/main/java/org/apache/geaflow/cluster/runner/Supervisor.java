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

package org.apache.geaflow.cluster.runner;

import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_AGENT_PORT;
import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_SUPERVISOR_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.AGENT_HTTP_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.FO_MAX_RESTARTS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import com.baidu.brpc.server.RpcServerOptions;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.rpc.impl.RpcServiceImpl;
import org.apache.geaflow.cluster.rpc.impl.SupervisorEndpoint;
import org.apache.geaflow.cluster.web.agent.AgentWebServer;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.rpc.ConfigurableServerOption;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Supervisor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Supervisor.class);
    private static final int DEFAULT_RETRIES = 3;

    private final RpcServiceImpl rpcService;
    private final CommandRunner mainRunner;
    private final Configuration configuration;
    private final int maxRestarts;
    private final Map<String, String> envMap;

    public Supervisor(String startCommand, Configuration configuration, boolean autoRestart) {
        this.configuration = configuration;
        this.maxRestarts = autoRestart ? configuration.getInteger(FO_MAX_RESTARTS) : 0;

        RpcServerOptions serverOptions = getServerOptions(configuration);
        int port = PortUtil.getPort(configuration.getInteger(SUPERVISOR_RPC_PORT));
        this.rpcService = new RpcServiceImpl(port, serverOptions);
        this.rpcService.addEndpoint(new SupervisorEndpoint(this));
        this.rpcService.startService();

        this.envMap = new HashMap<>();
        envMap.put(ENV_SUPERVISOR_PORT, String.valueOf(port));

        this.mainRunner = new CommandRunner(startCommand, maxRestarts, envMap, configuration);
        LOGGER.info("Start supervisor with maxRestarts: {}", maxRestarts);
    }

    private RpcServerOptions getServerOptions(Configuration configuration) {
        RpcServerOptions serverOptions = ConfigurableServerOption.build(configuration);
        serverOptions.setGlobalThreadPoolSharing(false);
        serverOptions.setIoThreadNum(1);
        serverOptions.setWorkThreadNum(2);
        return serverOptions;
    }

    public void start() {
        try {
            startAgent();
            startWorker();
        } catch (Throwable e) {
            StatsCollectorFactory.init(configuration).getExceptionCollector().reportException(
                ExceptionLevel.FATAL, e);
            throw e;
        }
    }

    public void restartWorker(int pid) {
        LOGGER.info("Restart worker process: {}", pid);
        stopWorker(pid);
        startWorker();
    }

    public void startWorker() {
        mainRunner.asyncStart();
    }

    public boolean isWorkerAlive() {
        Process process = mainRunner.getProcess();
        if (maxRestarts > 0 || process != null && process.isAlive()) {
            return true;
        }
        LOGGER.warn("Worker process {} is dead.", mainRunner.getProcessId());
        return false;
    }

    public void stopWorker() {
        mainRunner.stop();
    }

    public void stopWorker(int pid) {
        mainRunner.stop(pid);
    }

    public void startAgent() {
        RetryCommand.run(() -> {
            int agentPort = PortUtil.getPort(configuration.getInteger(AGENT_HTTP_PORT));
            envMap.put(ENV_AGENT_PORT, String.valueOf(agentPort));
            AgentWebServer server = new AgentWebServer(agentPort, configuration);
            server.start();
            return null;
        }, DEFAULT_RETRIES);
    }

    public void waitForTermination() {
        if (rpcService != null) {
            rpcService.waitTermination();
        }
    }

    public void stop() {
        if (rpcService != null) {
            rpcService.stopService();
        }
    }

}
