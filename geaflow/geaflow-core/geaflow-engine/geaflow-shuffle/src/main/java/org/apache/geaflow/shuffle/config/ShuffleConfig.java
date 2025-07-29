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

package org.apache.geaflow.shuffle.config;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CLIENT_THREADS_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_INITIAL_BACKOFF_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_MAX_BACKOFF_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_MAX_RETRY_TIMES;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CONNECT_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_CUSTOM_FRAME_DECODER_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_PREFER_DIRECT_BUFFER;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_RECEIVE_BUFFER_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SEND_BUFFER_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_BACKLOG;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_HOST;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_SERVER_THREADS_NUM;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.NETTY_THREAD_CACHE_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_BACKPRESSURE_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_COMPRESSION_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_EMIT_BUFFER_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_EMIT_QUEUE_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_CHANNEL_QUEUE_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_QUEUE_SIZE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FETCH_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FLUSH_BUFFER_SIZE_BYTES;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_FLUSH_BUFFER_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_STORAGE_TYPE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_WRITER_BUFFER_SIZE;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.shuffle.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleConfig.class);

    private final Configuration configuration;

    //////////////////////////////
    // Netty
    /// ///////////////////////////

    private final String serverAddress;
    private final int serverPort;
    // Connect timeout in milliseconds. Default 120 secs.
    private final int connectTimeoutMs;
    // fetch timeout in milliseconds. Default is 600 secs.
    private final int serverBacklog;
    private final int serverThreads;
    private final int clientThreads;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final int connectMaxRetryTimes;
    private final int connectInitBackoffMs;
    private final int connectMaxBackoffMs;
    private final boolean threadCacheEnabled;
    private final boolean preferDirectBuffer;
    private final boolean customFrameDecoderEnable;
    private final boolean enableBackpressure;

    //////////////////////////////
    // Read & Write
    /// ///////////////////////////

    private final boolean memoryPoolEnable;
    private final boolean compressionEnabled;

    //////////////////////////////
    // Read
    /// ///////////////////////////

    private final int fetchTimeoutMs;
    private final int fetchQueueSize;
    private final int channelQueueSize;


    //////////////////////////////
    // Write
    /// ///////////////////////////

    private final int emitQueueSize;
    private final int emitBufferSize;
    private final int maxBufferSizeBytes;
    private final int maxWriteBufferSize;
    private final int flushBufferTimeoutMs;
    private final StorageLevel storageLevel;

    public ShuffleConfig(Configuration config) {
        this.configuration = config;

        // netty
        this.serverAddress = config.getString(NETTY_SERVER_HOST);
        this.serverPort = config.getInteger(NETTY_SERVER_PORT);
        this.connectTimeoutMs = config.getInteger(NETTY_CONNECT_TIMEOUT_MS);
        this.serverBacklog = config.getInteger(NETTY_SERVER_BACKLOG);
        this.serverThreads = config.getInteger(NETTY_SERVER_THREADS_NUM);
        this.clientThreads = config.getInteger(NETTY_CLIENT_THREADS_NUM);
        this.receiveBufferSize = config.getInteger(NETTY_RECEIVE_BUFFER_SIZE);
        this.sendBufferSize = config.getInteger(NETTY_SEND_BUFFER_SIZE);
        this.connectMaxRetryTimes = config.getInteger(NETTY_CONNECT_MAX_RETRY_TIMES);
        this.connectInitBackoffMs = config.getInteger(NETTY_CONNECT_INITIAL_BACKOFF_MS);
        this.connectMaxBackoffMs = config.getInteger(NETTY_CONNECT_MAX_BACKOFF_MS);
        this.threadCacheEnabled = config.getBoolean(NETTY_THREAD_CACHE_ENABLE);
        this.preferDirectBuffer = config.getBoolean(NETTY_PREFER_DIRECT_BUFFER);
        this.customFrameDecoderEnable = config.getBoolean(NETTY_CUSTOM_FRAME_DECODER_ENABLE);

        // read & write
        this.memoryPoolEnable = config.getBoolean(SHUFFLE_MEMORY_POOL_ENABLE);
        this.compressionEnabled = config.getBoolean(SHUFFLE_COMPRESSION_ENABLE);
        this.enableBackpressure = config.getBoolean(SHUFFLE_BACKPRESSURE_ENABLE);

        // read
        this.fetchTimeoutMs = config.getInteger(SHUFFLE_FETCH_TIMEOUT_MS);
        this.fetchQueueSize = config.getInteger(SHUFFLE_FETCH_QUEUE_SIZE);
        this.channelQueueSize = config.getInteger(SHUFFLE_FETCH_CHANNEL_QUEUE_SIZE);

        // write
        this.emitQueueSize = config.getInteger(SHUFFLE_EMIT_QUEUE_SIZE);
        this.emitBufferSize = config.getInteger(SHUFFLE_EMIT_BUFFER_SIZE);
        this.maxBufferSizeBytes = config.getInteger(SHUFFLE_FLUSH_BUFFER_SIZE_BYTES);
        this.maxWriteBufferSize = config.getInteger(SHUFFLE_WRITER_BUFFER_SIZE);
        this.flushBufferTimeoutMs = config.getInteger(SHUFFLE_FLUSH_BUFFER_TIMEOUT_MS);
        this.storageLevel = StorageLevel.valueOf(config.getString(SHUFFLE_STORAGE_TYPE));

        LOGGER.info("init shuffle config: {}", config);
    }

    public Configuration getConfig() {
        return this.configuration;
    }

    public String getServerAddress() {
        return this.serverAddress;
    }

    public int getServerPort() {
        return this.serverPort;
    }

    public int getConnectTimeoutMs() {
        return this.connectTimeoutMs;
    }

    /**
     * Requested maximum length of the queue of incoming connections.
     *
     * @return server back log.
     */
    public int getServerConnectBacklog() {
        return this.serverBacklog;
    }

    /**
     * Number of threads used in the server thread pool. Default to 4, and 0 means 2x#cores.
     */
    public int getServerThreadsNum() {
        return this.serverThreads;
    }

    /**
     * Number of threads used in the client thread pool. Default to 4, and 0 means 2x#cores.
     */
    public int getClientNumThreads() {
        return this.clientThreads;
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     * latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     * buffer size should be ~ 1.25MB
     */
    public int getReceiveBufferSize() {
        return this.receiveBufferSize;
    }

    public int getSendBufferSize() {
        return this.sendBufferSize;
    }

    public int getConnectMaxRetries() {
        return this.connectMaxRetryTimes;
    }

    public int getConnectInitialBackoffMs() {
        return this.connectInitBackoffMs;
    }

    public int getConnectMaxBackoffMs() {
        return this.connectMaxBackoffMs;
    }

    public boolean isThreadCacheEnabled() {
        return this.threadCacheEnabled;
    }

    public boolean preferDirectBuffer() {
        return this.preferDirectBuffer;
    }

    public boolean enableCustomFrameDecoder() {
        return this.customFrameDecoderEnable;
    }

    public boolean isMemoryPoolEnable() {
        return this.memoryPoolEnable;
    }

    public boolean isCompressionEnabled() {
        return this.compressionEnabled;
    }

    public int getFetchTimeoutMs() {
        return this.fetchTimeoutMs;
    }

    public int getFetchQueueSize() {
        return this.fetchQueueSize;
    }

    public int getChannelQueueSize() {
        return channelQueueSize;
    }

    public int getEmitQueueSize() {
        return this.emitQueueSize;
    }

    public int getEmitBufferSize() {
        return this.emitBufferSize;
    }

    public int getMaxBufferSizeBytes() {
        return this.maxBufferSizeBytes;
    }

    public boolean isBackpressureEnabled() {
        return enableBackpressure;
    }

    public int getMaxWriteBufferSize() {
        return maxWriteBufferSize;
    }

    public int getFlushBufferTimeoutMs() {
        return this.flushBufferTimeoutMs;
    }

    public StorageLevel getStorageLevel() {
        return this.storageLevel;
    }
}
