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

package org.apache.geaflow.cluster.web.agent;

import static org.apache.geaflow.cluster.constants.ClusterConstants.AGENT_PROFILER_PATH;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.LOG_DIR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.PROFILER_FILENAME_EXTENSION;

import org.apache.geaflow.cluster.web.agent.handler.FlameGraphRestHandler;
import org.apache.geaflow.cluster.web.agent.handler.LogRestHandler;
import org.apache.geaflow.cluster.web.agent.handler.ThreadDumpRestHandler;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentWebServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentWebServer.class);

    private static final String AGENT_SERVER_NAME = "agent-jetty-server";

    private static final int DEFAULT_ACCEPT_QUEUE_SIZE = 8;

    private final Server server;

    private final int httpPort;

    private final QueuedThreadPool threadPool;

    private final ScheduledExecutorScheduler serverExecutor;

    private final Object lock = new Object();

    public AgentWebServer(int httpPort, Configuration configuration) {
        this(httpPort, configuration.getString(LOG_DIR),
            configuration.getString(AGENT_PROFILER_PATH),
            configuration.getString(PROFILER_FILENAME_EXTENSION),
            configuration.getString(JOB_WORK_PATH));
    }

    public AgentWebServer(int httpPort, String runtimeLogDirPath, String flameGraphProfilerPath,
                          String flameGraphFileNameExtension, String agentDir) {
        this.httpPort = httpPort;
        threadPool = new QueuedThreadPool();
        threadPool.setDaemon(true);
        threadPool.setName(AGENT_SERVER_NAME);
        server = new Server(threadPool);

        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks(true);
        errorHandler.setServer(server);
        server.addBean(errorHandler);

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new LogRestHandler(runtimeLogDirPath));
        resourceConfig.register(new FlameGraphRestHandler(flameGraphProfilerPath, flameGraphFileNameExtension, agentDir));
        resourceConfig.register(new ThreadDumpRestHandler(agentDir));

        ServletContextHandler handler = new ServletContextHandler(
            ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(resourceConfig)), "/rest/*");
        handler.addServlet(new ServletHolder(new DefaultServlet()), "/");

        server.setHandler(handler);

        serverExecutor = new ScheduledExecutorScheduler("jetty-scheduler", true);
    }

    public void start() {
        try {
            ServerConnector connector = newConnector(server, serverExecutor, null, httpPort);
            connector.setName(AGENT_SERVER_NAME);
            server.addConnector(connector);

            int minThreads = 1;
            minThreads += connector.getAcceptors() * 2;
            threadPool.setMaxThreads(Math.max(threadPool.getMaxThreads(), minThreads));

            server.start();
            LOGGER.info("Jetty Server started: {}.", httpPort);
        } catch (Exception e) {
            LOGGER.error("Jetty server failed.", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public void await() throws InterruptedException {
        LOGGER.info("Wait for agent jetty server stopped.");
        synchronized (lock) {
            lock.wait();
        }
    }

    public void stop() {
        try {
            server.stop();
            if (threadPool.isStarted()) {
                threadPool.stop();
            }
            if (serverExecutor.isStarted()) {
                serverExecutor.stop();
            }
            synchronized (lock) {
                lock.notify();
            }
        } catch (Exception e) {
            LOGGER.warn("Stop jetty server failed.", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    private ServerConnector newConnector(Server server, ScheduledExecutorScheduler serverExecutor,
                                         String hostName, int port) throws Exception {
        ConnectionFactory[] connectionFactories = new ConnectionFactory[]{
            new HttpConnectionFactory()};
        ServerConnector connector = new ServerConnector(server, null, serverExecutor, null, -1, -1,
            connectionFactories);
        connector.setHost(hostName);
        connector.setPort(port);
        connector.start();
        connector.setAcceptQueueSize(Math.min(connector.getAcceptors(), DEFAULT_ACCEPT_QUEUE_SIZE));
        return connector;
    }


}
