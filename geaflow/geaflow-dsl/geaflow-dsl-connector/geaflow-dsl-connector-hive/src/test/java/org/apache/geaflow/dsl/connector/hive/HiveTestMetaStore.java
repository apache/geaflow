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

package org.apache.geaflow.dsl.connector.hive;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTestMetaStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTestMetaStore.class);

    private final String hiveLocation;

    private final HiveConf hiveConf;

    private final ExecutorService executorService;
    private TServer tServer;

    public HiveTestMetaStore(HiveConf hiveConf, String hiveLocation) {
        this.hiveLocation = hiveLocation;
        this.hiveConf = hiveConf;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void start() throws IOException {
        int metastorePort = hiveConf.getIntVar(ConfVars.METASTORE_SERVER_PORT);
        tServer = startMetaStore(hiveConf);
        waitForServer(metastorePort);
        LOGGER.info("Hive metastore server has started at port:{}", metastorePort);
    }

    private static void waitForServer(int serverPort) {
        final long endTime = System.currentTimeMillis() + (long) 20000;
        try {
            while (System.currentTimeMillis() < endTime) {
                SocketChannel channel = null;
                try {
                    channel = SocketChannel.open(new InetSocketAddress("localhost", serverPort));
                    LOGGER.info("Server started at port {}", serverPort);
                    return;
                } catch (ConnectException e) {
                    LOGGER.info("Waiting for server to start...");
                    Thread.sleep(1000);
                } finally {
                    if (channel != null) {
                        channel.close();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to start server at port: " + serverPort);
        }
    }

    public void stop() {
        if (tServer != null) {
            try {
                tServer.stop();
                LOGGER.info("MetaStore sever has stop");
            } catch (Exception e) {
                LOGGER.error("stop meta store failed", e);
            }
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
        try {
            FileUtils.deleteDirectory(new File(hiveLocation));
        } catch (IOException e) {
            LOGGER.warn("fail to clear hive location: " + hiveLocation);
        }
    }

    private TServer startMetaStore(HiveConf conf) throws IOException {
        try {
            int port = conf.getIntVar(HiveConf.ConfVars.METASTORE_SERVER_PORT);
            int minWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
            int maxWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMAXTHREADS);
            boolean tcpKeepAlive = conf.getBoolVar(HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE);
            boolean useFramedTransport = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);

            InetSocketAddress address = new InetSocketAddress("localhost", port);
            TServerTransport serverTransport = tcpKeepAlive ? new TServerSocketKeepAlive(address) :
                new TServerSocket(address);

            TProcessor processor;
            TTransportFactory transFactory;
            HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler(
                "Test metastore handler", conf, false);
            IHMSHandler handler = RetryingHMSHandler.getProxy(conf, baseHandler, true);

            transFactory = useFramedTransport ? new TFramedTransport.Factory() : new TTransportFactory();
            processor = new TSetIpAddressProcessor<>(handler);

            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport).processor(processor)
                .transportFactory(transFactory).protocolFactory(new TBinaryProtocol.Factory())
                .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads);

            final TServer tServer = new TThreadPoolServer(args);
            executorService.submit(tServer::serve);
            return tServer;
        } catch (Throwable x) {
            throw new IOException(x);
        }
    }

    private static final class TServerSocketKeepAlive extends TServerSocket {
        public TServerSocketKeepAlive(InetSocketAddress address) throws TTransportException {
            super(address, 0);
        }

        @Override
        protected TSocket acceptImpl() throws TTransportException {
            TSocket ts = super.acceptImpl();
            try {
                ts.getSocket().setKeepAlive(true);
            } catch (SocketException e) {
                throw new TTransportException(e);
            }
            return ts;
        }
    }
}
