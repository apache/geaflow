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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.cluster.util.SystemExitSignalCatcher;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ComponentUncaughtExceptionHandlerTest {

    private static SecurityManager securityManager;
    private static AtomicBoolean hasException = new AtomicBoolean(false);


    @BeforeClass
    public void before() {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new SystemExitSignalCatcher(hasException));
    }

    @AfterClass
    public void after() {
        System.setSecurityManager(securityManager);
    }

    @BeforeMethod
    public void beforeMethod() {
        hasException.set(false);
    }

    @Test
    public void testHandleExceptionInThreadPool() throws InterruptedException {

        ComponentExceptionSupervisor.getInstance();
        ExecutorService executorService = Executors.newFixedThreadPool(2,
            ThreadUtil.namedThreadFactory(true, "test-handler", new ComponentUncaughtExceptionHandler()));

        executorService.execute(() -> {
            throw new RuntimeException("test exception");
        });
        executorService.execute(ComponentExceptionSupervisor.getInstance());
        // wait async thread catch and handle exception
        Thread.sleep(100);
        Assert.assertTrue(hasException.get());
    }
}
