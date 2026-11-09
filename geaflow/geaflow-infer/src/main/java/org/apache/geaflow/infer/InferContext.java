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

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_INIT_TIMEOUT_SEC;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_USER_TRANSFORM_CLASSNAME;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.exchange.DataExchangeContext;
import org.apache.geaflow.infer.exchange.impl.InferDataBridgeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferContext<OUT> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferContext.class);
    
    private static final ScheduledExecutorService SCHEDULER = 
        new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "infer-context-monitor");
            t.setDaemon(true);
            return t;
        });
    
    private final Configuration config;
    private final DataExchangeContext shareMemoryContext;
    private final String userDataTransformClass;
    private final String sendQueueKey;

    private final String receiveQueueKey;
    private InferTaskRunImpl inferTaskRunner;
    private InferDataBridgeImpl<OUT> dataBridge;

    public InferContext(Configuration config) {
        this.config = config;
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
        try {
            dataBridge.write(feature);
            return dataBridge.read();
        } catch (Exception e) {
            inferTaskRunner.stop();
            LOGGER.error("model infer read result error, python process stopped", e);
            throw new GeaflowRuntimeException("receive infer result exception", e);
        }
    }


    private InferEnvironmentContext getInferEnvironmentContext() {
        long startTime = System.currentTimeMillis();
        int timeoutSec = config.getInteger(INFER_ENV_INIT_TIMEOUT_SEC);
        long timeoutMs = timeoutSec * 1000L;
        
        //  确保 InferEnvironmentManager 已被初始化和启动
        InferEnvironmentManager inferManager = InferEnvironmentManager.buildInferEnvironmentManager(config);
        inferManager.createEnvironment();
        
        CountDownLatch initLatch = new CountDownLatch(1);
        
        // Schedule periodic checks for environment initialization
        ScheduledExecutorService localScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "infer-env-check-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        
        try {
            localScheduler.scheduleAtFixedRate(() -> {
                long elapsedMs = System.currentTimeMillis() - startTime;
                
                if (elapsedMs > timeoutMs) {
                    LOGGER.error(
                        "InferContext initialization timeout after {}ms. Timeout configured: {}s",
                        elapsedMs, timeoutSec);
                    initLatch.countDown();
                    throw new GeaflowRuntimeException(
                        "InferContext initialization timeout: exceeded " + timeoutSec + " seconds");
                }
                
                try {
                    InferEnvironmentManager.checkError();
                    boolean initFinished = InferEnvironmentManager.checkInferEnvironmentStatus();
                    if (initFinished) {
                        LOGGER.debug("InferContext environment initialized in {}ms",
                            System.currentTimeMillis() - startTime);
                        initLatch.countDown();
                    }
                } catch (Exception e) {
                    LOGGER.error("Error checking infer environment status", e);
                    initLatch.countDown();
                }
            }, 100, 100, TimeUnit.MILLISECONDS);
            
            // Wait for initialization with timeout
            boolean finished = initLatch.await(timeoutSec, TimeUnit.SECONDS);
            
            if (!finished) {
                throw new GeaflowRuntimeException(
                    "InferContext initialization timeout: exceeded " + timeoutSec + " seconds");
            }
            
            // Final check for errors
            InferEnvironmentManager.checkError();
            
            LOGGER.info("InferContext environment initialized in {}ms",
                System.currentTimeMillis() - startTime);
            return InferEnvironmentManager.getEnvironmentContext();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GeaflowRuntimeException(
                "InferContext initialization interrupted", e);
        } finally {
            localScheduler.shutdownNow();
        }
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

    @Override
    public void close() {
        if (inferTaskRunner != null) {
            inferTaskRunner.stop();
            LOGGER.info("infer task stop after close");
        }
    }
}
