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

package org.apache.geaflow.store.data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.utils.ExecutorUtil;
import org.apache.geaflow.common.utils.ExecutorUtil.ExceptionHandler;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncFlushMultiVersionedBuffer<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFlushMultiVersionedBuffer.class);
    private static final int SLEEP_MILLI_SECOND = 100;

    private final boolean deepCopy;
    private final int bufferNum;
    private final int bufferSize;
    private final Consumer<GraphWriteMultiVersionedBuffer<K, VV, EV>> flushFun;
    private final ISerializer serializer;
    private final ExceptionHandler flushError;
    private List<GraphWriteMultiVersionedBuffer<K, VV, EV>> buffers;
    private int curWriteBufferIdx;
    private ThreadPoolExecutor flushService;

    protected AtomicLong writeCounter = new AtomicLong(0);
    protected AtomicLong flushCounter = new AtomicLong(0);
    protected volatile Throwable exp;

    public AsyncFlushMultiVersionedBuffer(Configuration config,
                                          Consumer<GraphWriteMultiVersionedBuffer<K, VV, EV>> flushFun,
                                          ISerializer serializer) {
        this.flushFun = flushFun;
        this.serializer = serializer;
        this.deepCopy = config.getBoolean(StateConfigKeys.STATE_WRITE_BUFFER_DEEP_COPY);
        this.bufferNum = config.getInteger(StateConfigKeys.STATE_WRITE_BUFFER_NUMBER);
        this.bufferSize = config.getInteger(StateConfigKeys.STATE_WRITE_BUFFER_SIZE);
        this.flushError = exception -> {
            exp = exception;
            LOGGER.error("flush error", exception);
        };
        initBuffer();
    }

    private void initBuffer() {
        this.buffers = new ArrayList<>(bufferNum);
        for (int i = 0; i < bufferNum; i++) {
            this.buffers.add(new GraphWriteMultiVersionedBuffer<>(bufferSize));
        }
        this.curWriteBufferIdx = 0;
        this.flushService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(bufferNum * 2),
            new BasicThreadFactory.Builder().namingPattern("flush-%d").build());
    }

    public void flush() {
        for (int i = 0; i < bufferNum; i++) {
            if (!buffers.get(i).isFlushing()) {
                tryFlushBuffer(i, true);
            }
        }

        ExecutorUtil.spinLockMs(() -> flushCounter.get() == writeCounter.get(),
            this::exceptionCheck, SLEEP_MILLI_SECOND);
        // LOGGER.info("flushCount {}", flushCounter.get());
        flushCounter.set(0);
        writeCounter.set(0);
        exceptionCheck();
    }

    private void exceptionCheck() {
        if (exp != null) {
            LOGGER.error("encounter exception");
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError(exp.getMessage()), exp);
        }
    }

    private void tryFlushBuffer(int idx, boolean force) {
        GraphWriteMultiVersionedBuffer<K, VV, EV> buffer = buffers.get(idx);
        boolean needFlush = buffer.needFlush() || (force && buffer.getSize() > 0);
        if (!needFlush) {
            return;
        }
        buffer.setFlushing();
        exceptionCheck();
        ExecutorUtil.execute(flushService, () -> flushWriteBuffer(buffer), flushError);
        int toWriteBufferIdx = (idx + 1) % buffers.size();
        ExecutorUtil.spinLockMs(() -> !buffers.get(toWriteBufferIdx).needFlush(),
            this::exceptionCheck, SLEEP_MILLI_SECOND);

        curWriteBufferIdx = toWriteBufferIdx;
    }

    private void flushWriteBuffer(GraphWriteMultiVersionedBuffer<K, VV, EV> buffer) {
        flushFun.accept(buffer);
        flushCounter.addAndGet(buffer.getSize());
        buffer.clear();
    }

    public IVertex<K, VV> readBufferedVertex(long version, K id) {
        for (int i = 0; i < bufferNum; i++) {
            int idx = (bufferNum + curWriteBufferIdx - i) % bufferNum;
            IVertex<K, VV> vertex = buffers.get(idx).getVertex(version, id);
            if (vertex != null) {
                return deepCopy ? serializer.copy(vertex) : vertex;
            }
        }
        return null;
    }

    public List<IEdge<K, EV>> readBufferedEdges(long version, K srcId) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        for (int i = 0; i < bufferNum; i++) {
            List<IEdge<K, EV>> edgeList = buffers.get(i).getEdges(version, srcId);
            if (edgeList != null && edgeList.size() > 0) {
                list.addAll(deepCopy ? serializer.copy(edgeList) : edgeList);
            }
        }
        return list;
    }

    public void addVertex(long version, IVertex<K, VV> vertex) {
        writeCounter.incrementAndGet();
        buffers.get(curWriteBufferIdx).addVertex(version, vertex);
        tryFlushBuffer(curWriteBufferIdx, false);
    }

    public void addEdge(long version, IEdge<K, EV> edge) {
        writeCounter.incrementAndGet();
        buffers.get(curWriteBufferIdx).addEdge(version, edge);
        tryFlushBuffer(curWriteBufferIdx, false);
    }

    public void close() {
        flush();
        flushService.shutdown();
        buffers.forEach(GraphWriteMultiVersionedBuffer::clear);
        buffers.clear();
    }
}
