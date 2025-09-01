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

public class AsyncFlushBuffer<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFlushBuffer.class);
    private static final int SLEEP_MILLI_SECOND = 100;

    private final boolean deepCopy;
    private final int bufferNum;
    private final int bufferSize;
    private final Consumer<GraphWriteBuffer<K, VV, EV>> flushFun;
    private final ISerializer serializer;
    private final ExceptionHandler flushError;
    private List<GraphWriteBuffer<K, VV, EV>> buffers;
    private int curWriteBufferIdx;
    private ThreadPoolExecutor flushService;

    protected AtomicLong writeCounter = new AtomicLong(0);

    protected AtomicLong flushCounter = new AtomicLong(0);

    protected volatile Throwable exp;

    public AsyncFlushBuffer(Configuration config,
                            Consumer<GraphWriteBuffer<K, VV, EV>> flushFun,
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
            this.buffers.add(new GraphWriteBuffer<>(bufferSize));
        }
        this.curWriteBufferIdx = 0;
        this.flushService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(bufferNum + 1), new BasicThreadFactory.Builder().namingPattern("flush-%d").build());
    }

    public void flush() {
        if (writeCounter.get() == 0) {
            return;
        }
        for (int i = 0; i < bufferNum; i++) {
            if (!buffers.get(i).isFlushing()) {
                tryFlushBuffer(i, true);
            }
        }

        ExecutorUtil.spinLockMs(() -> flushCounter.get() == writeCounter.get(),
            this::exceptionCheck, SLEEP_MILLI_SECOND);
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
        GraphWriteBuffer<K, VV, EV> buffer = buffers.get(idx);
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

    private void flushWriteBuffer(GraphWriteBuffer<K, VV, EV> buffer) {
        flushFun.accept(buffer);
        flushCounter.addAndGet(buffer.getSize());
        buffer.clear();
    }

    public IVertex<K, VV> readBufferedVertex(K id) {
        for (int i = 0; i < bufferNum; i++) {
            int idx = (bufferNum + curWriteBufferIdx - i) % bufferNum;
            GraphWriteBuffer<K, VV, EV> buffer = buffers.get(idx);
            IVertex<K, VV> vertex = buffer.getVertexId2Vertex().get(id);
            if (vertex != null) {
                return deepCopy ? serializer.copy(vertex) : vertex;
            }
        }
        return null;
    }

    public List<IEdge<K, EV>> readBufferedEdges(K srcId) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        for (int i = 0; i < bufferNum; i++) {
            GraphWriteBuffer<K, VV, EV> buffer = buffers.get(i);
            List<IEdge<K, EV>> edgeList = buffer.getVertexId2Edges().get(srcId);
            if (edgeList != null) {
                list.addAll(deepCopy ? serializer.copy(edgeList) : edgeList);
            }
        }
        return list;
    }

    public void addVertex(IVertex<K, VV> vertex) {
        writeCounter.incrementAndGet();
        buffers.get(curWriteBufferIdx).addVertex(vertex);
        tryFlushBuffer(curWriteBufferIdx, false);
    }

    public void addEdge(IEdge<K, EV> edge) {
        writeCounter.incrementAndGet();
        buffers.get(curWriteBufferIdx).addEdge(edge);
        tryFlushBuffer(curWriteBufferIdx, false);
    }

    public void close() {
        flushService.shutdown();
        buffers.forEach(GraphWriteBuffer::clear);
        buffers.clear();
    }
}
