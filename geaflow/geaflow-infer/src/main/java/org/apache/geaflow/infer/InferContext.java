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
package org.apache.geaflow.infer;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_USER_TRANSFORM_CLASSNAME;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.exchange.DataExchangeContext;
import org.apache.geaflow.infer.exchange.impl.InferDataBridgeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferContext<OUT> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferContext.class);
    private final DataExchangeContext shareMemoryContext;
    private final String userDataTransformClass;
    private final String sendQueueKey;

    private final String receiveQueueKey;
    private InferTaskRunImpl inferTaskRunner;
    private InferDataBridgeImpl<OUT> dataBridge;
    private final ReentrantLock inferLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean broken;

    public InferContext(Configuration config) {
        this.shareMemoryContext = new DataExchangeContext(config);
        this.receiveQueueKey = shareMemoryContext.getReceiveQueueKey();
        this.sendQueueKey = shareMemoryContext.getSendQueueKey();
        this.userDataTransformClass = config.getString(INFER_ENV_USER_TRANSFORM_CLASSNAME);
        Preconditions.checkNotNull(userDataTransformClass,
            INFER_ENV_USER_TRANSFORM_CLASSNAME.getKey() + " param must be not null");
        this.dataBridge = new InferDataBridgeImpl<>(shareMemoryContext);
        init();
    }

    private void init() {
        try {
            InferEnvironmentContext inferEnvironmentContext = getInferEnvironmentContext();
            runInferTask(inferEnvironmentContext);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("infer context init failed", e);
        }
    }

    public OUT infer(Object... feature) throws Exception {
        ensureAvailable();
        inferLock.lock();
        try {
            ensureAvailable();
            dataBridge.write(feature);
            return dataBridge.read();
        } catch (Exception e) {
            broken = true;
            stopInferTask();
            LOGGER.error("model infer read result error, python process stopped", e);
            throw new GeaflowRuntimeException("receive infer result exception", e);
        } finally {
            inferLock.unlock();
        }
    }

    private InferEnvironmentContext getInferEnvironmentContext() {
        boolean initFinished = InferEnvironmentManager.checkInferEnvironmentStatus();
        while (!initFinished) {
            InferEnvironmentManager.checkError();
            initFinished = InferEnvironmentManager.checkInferEnvironmentStatus();
        }
        return InferEnvironmentManager.getEnvironmentContext();
    }

    private void runInferTask(InferEnvironmentContext inferEnvironmentContext) {
        inferTaskRunner = new InferTaskRunImpl(inferEnvironmentContext);
        List<String> runCommands = new ArrayList<>();
        runCommands.add(inferEnvironmentContext.getPythonExec());
        runCommands.add(inferEnvironmentContext.getInferScript());
        runCommands.add(inferEnvironmentContext.getInferTFClassNameParam(this.userDataTransformClass));
        runCommands.add(inferEnvironmentContext.getInferShareMemoryInputParam(receiveQueueKey));
        runCommands.add(inferEnvironmentContext.getInferShareMemoryOutputParam(sendQueueKey));
        inferTaskRunner.run(runCommands);
    }

    public boolean isBroken() {
        return broken;
    }

    private void ensureAvailable() {
        if (closed.get()) {
            throw new GeaflowRuntimeException("infer context already closed");
        }
        if (broken) {
            throw new GeaflowRuntimeException("infer context is broken");
        }
    }

    private void stopInferTask() {
        if (inferTaskRunner != null) {
            inferTaskRunner.stop();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        shareMemoryContext.markFinished();
        stopInferTask();
        inferLock.lock();
        try {
            if (dataBridge != null) {
                dataBridge.close();
                dataBridge = null;
            }
            shareMemoryContext.close();
            LOGGER.info("infer task stop after close");
        } catch (Exception e) {
            throw new GeaflowRuntimeException("close infer context failed", e);
        } finally {
            inferLock.unlock();
        }
    }
}
