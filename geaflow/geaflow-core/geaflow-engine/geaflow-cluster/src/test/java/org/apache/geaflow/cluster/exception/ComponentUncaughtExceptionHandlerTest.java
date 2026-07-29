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

package org.apache.geaflow.cluster.exception;

import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ComponentUncaughtExceptionHandlerTest {

    @Test
    public void testHandleExceptionInThreadPool() throws InterruptedException {

        AtomicInteger exitCode = new AtomicInteger();
        CountDownLatch exitCalled = new CountDownLatch(1);
        ComponentExceptionSupervisor supervisor = new ComponentExceptionSupervisor(code -> {
            exitCode.set(code);
            exitCalled.countDown();
        });
        ComponentExceptionSupervisor.setInstance(supervisor);
        ExecutorService executorService = Executors.newFixedThreadPool(2,
            ThreadUtil.namedThreadFactory(true, "test-handler", new ComponentUncaughtExceptionHandler()));

        try {
            executorService.execute(() -> {
                throw new RuntimeException("test exception");
            });
            executorService.execute(supervisor);

            Assert.assertTrue(exitCalled.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(exitCode.get(), EXIT_CODE);
        } finally {
            supervisor.shutdown();
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
