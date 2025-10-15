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

package org.apache.geaflow.analytics.service.server.http;

import org.apache.geaflow.analytics.service.server.AbstractAnalyticsServiceServer;
import org.apache.geaflow.analytics.service.server.http.handler.HttpAnalyticsServiceHandler;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.pipeline.service.IPipelineServiceExecutorContext;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpAnalyticsServiceServer extends AbstractAnalyticsServiceServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAnalyticsServiceServer.class);
    private static final String SERVER_NAME = "analytics-service-server";
    private static final String SERVER_SCHEDULER = "analytics-service-server-scheduler";
    private static final String QUERY_EXECUTE_REST_API = "/rest/analytics/query/execute";

    private HttpAnalyticsServiceHandler httpHandler;
    private Server server;
    private QueuedThreadPool threadPool;
    private ScheduledExecutorScheduler serverExecutor;

    @Override
    public void init(IPipelineServiceExecutorContext context) {
        super.init(context);

        this.httpHandler = new HttpAnalyticsServiceHandler(requestBlockingQueue,
            responseBlockingMap, semaphore);
        this.threadPool = new QueuedThreadPool();
        this.threadPool.setDaemon(true);
        this.threadPool.setName(SERVER_NAME);
        this.server = new Server(threadPool);
        this.port = PortUtil.getPort(port);
        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks(true);
        errorHandler.setServer(this.server);
        this.server.addBean(errorHandler);

        this.serverExecutor = new ScheduledExecutorScheduler(SERVER_SCHEDULER, true);
    }

    @Override
    public void startServer() {
        // Jetty's processing collection.
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        server.setHandler(contexts);

        // Add servlet.
        ServletContextHandler contextHandler = new ServletContextHandler(
            ServletContextHandler.SESSIONS);
        contextHandler.addServlet(new ServletHolder(httpHandler), QUERY_EXECUTE_REST_API);
        contexts.addHandler(contextHandler);

        try {
            ServerConnector connector = newConnector(server, serverExecutor, null, port);
            connector.setName(SERVER_NAME);
            server.addConnector(connector);

            int minThreads = 1;
            minThreads += connector.getAcceptors() * 2;
            threadPool.setMaxThreads(Math.max(threadPool.getMaxThreads(), minThreads));
            server.start();
            String hostIpAddress = ProcessUtil.getHostIp();
            LOGGER.info("Http analytics Server started: ip {}, port {}", hostIpAddress, port);
        } catch (Exception e) {
            LOGGER.error("Http analytics Server start failed:", e);
            throw new GeaflowRuntimeException(e);
        }
        waitForExecuted();
    }

    @Override
    public void stopServer() {
        try {
            super.stopServer();
            server.stop();
            if (threadPool.isStarted()) {
                threadPool.stop();
            }
            if (serverExecutor.isStarted()) {
                serverExecutor.stop();
            }
        } catch (Exception e) {
            LOGGER.warn("stop analytics server failed", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_http;
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
        connector.setAcceptQueueSize(Math.min(connector.getAcceptors(), maxRequests));
        return connector;
    }

}
