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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class InferContextPoolTest {

    @AfterMethod
    public void tearDown() {
        InferContextPool.closeAll();
        InferContextPool.resetContextFactoryForTest();
    }

    @Test
    public void testBorrowCreatesDifferentContextsForConcurrentLeases() {
        AtomicInteger created = new AtomicInteger();
        InferContextPool.setContextFactoryForTest(config -> mockContext(created.incrementAndGet()));
        Configuration config = buildPoolConfig();

        try (InferContextLease<Object> first = InferContextPool.borrow(config);
             InferContextLease<Object> second = InferContextPool.borrow(config)) {
            assertNotSame(first, second);
            assertEquals(created.get(), 2);
        }
    }

    @Test
    public void testBorrowReusesStableConfigFingerprint() throws Exception {
        AtomicInteger created = new AtomicInteger();
        CopyOnWriteArrayList<InferContext<Object>> contexts = new CopyOnWriteArrayList<>();
        InferContextPool.setContextFactoryForTest(config -> {
            InferContext<Object> context = mockContext(created.incrementAndGet());
            contexts.add(context);
            return context;
        });

        Configuration firstConfig = buildPoolConfig();
        Configuration secondConfig = new Configuration(new LinkedHashMap<String, String>() {{
            put("k2", "v2");
            put("k1", "v1");
            put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE.getKey(), "1");
            put(FrameworkConfigKeys.INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC.getKey(), "1");
            put(FrameworkConfigKeys.INFER_ENV_SHARE_MEMORY_QUEUE_SIZE.getKey(), "1024");
            put(ExecutionConfigKeys.JOB_WORK_PATH.getKey(), System.getProperty("java.io.tmpdir"));
            put(FileConfigKeys.USER_NAME.getKey(), "tester");
            put(ExecutionConfigKeys.JOB_UNIQUE_ID.getKey(), "job");
        }});
        secondConfig.setMasterId(firstConfig.getMasterId());
        firstConfig.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE, "1");

        CountDownLatch borrowStarted = new CountDownLatch(1);
        CountDownLatch borrowFinished = new CountDownLatch(1);
        AtomicInteger borrowedCount = new AtomicInteger();

        try (InferContextLease<Object> first = InferContextPool.borrow(firstConfig)) {
            Thread waiter = new Thread(() -> {
                borrowStarted.countDown();
                try (InferContextLease<Object> ignored = InferContextPool.borrow(secondConfig)) {
                    borrowedCount.incrementAndGet();
                } finally {
                    borrowFinished.countDown();
                }
            });
            waiter.start();
            assertTrue(borrowStarted.await(1, TimeUnit.SECONDS));
            Thread.sleep(100L);
            assertEquals(created.get(), 1);
            assertEquals(borrowedCount.get(), 0);
        }

        assertTrue(borrowFinished.await(1, TimeUnit.SECONDS));
        assertEquals(borrowedCount.get(), 1);
        assertEquals(created.get(), 1);
        assertEquals(contexts.size(), 1);
    }

    @Test
    public void testBorrowReusesContextAcrossSequentialLeases() throws Exception {
        AtomicInteger created = new AtomicInteger();
        InferContextPool.setContextFactoryForTest(config -> mockContext(created.incrementAndGet()));
        Configuration config = buildPoolConfig();
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE, "1");

        try (InferContextLease<Object> first = InferContextPool.borrow(config)) {
            assertEquals(first.infer("value"), Integer.valueOf(1));
        }

        try (InferContextLease<Object> second = InferContextPool.borrow(config)) {
            assertEquals(second.infer("value"), Integer.valueOf(1));
        }

        assertEquals(created.get(), 1);
    }

    @Test
    public void testBorrowBlocksUntilLeaseReturned() throws Exception {
        AtomicInteger created = new AtomicInteger();
        InferContextPool.setContextFactoryForTest(config -> mockContext(created.incrementAndGet()));
        Configuration config = buildPoolConfig();
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE, "1");
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC, "1");

        CountDownLatch borrowStarted = new CountDownLatch(1);
        CountDownLatch borrowFinished = new CountDownLatch(1);
        AtomicInteger borrowedCount = new AtomicInteger();

        try (InferContextLease<Object> first = InferContextPool.borrow(config)) {
            Thread waiter = new Thread(() -> {
                borrowStarted.countDown();
                try (InferContextLease<Object> ignored = InferContextPool.borrow(config)) {
                    borrowedCount.incrementAndGet();
                } finally {
                    borrowFinished.countDown();
                }
            });
            waiter.start();
            assertTrue(borrowStarted.await(1, TimeUnit.SECONDS));
            Thread.sleep(100L);
            assertEquals(borrowedCount.get(), 0);
        }

        assertTrue(borrowFinished.await(1, TimeUnit.SECONDS));
        assertEquals(borrowedCount.get(), 1);
        assertEquals(created.get(), 1);
    }

    @Test
    public void testBorrowTimeoutWhenPoolExhausted() {
        AtomicInteger created = new AtomicInteger();
        InferContextPool.setContextFactoryForTest(config -> mockContext(created.incrementAndGet()));
        Configuration config = buildPoolConfig();
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE, "1");
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC, "0");

        try (InferContextLease<Object> ignored = InferContextPool.borrow(config)) {
            assertThrows(RuntimeException.class, () -> InferContextPool.borrow(config));
        }
        assertEquals(created.get(), 1);
    }

    @Test
    public void testBrokenContextIsClosedInsteadOfReused() throws Exception {
        AtomicInteger created = new AtomicInteger();
        InferContext<Object> brokenContext = mock(InferContext.class);
        doAnswer(invocation -> {
            throw new RuntimeException("broken");
        }).when(brokenContext).infer(org.mockito.Matchers.anyVararg());
        doAnswer(invocation -> true).when(brokenContext).isBroken();
        InferContext<Object> healthyContext = mockContext(2);
        InferContextPool.setContextFactoryForTest(config ->
            created.getAndIncrement() == 0 ? brokenContext : healthyContext);

        Configuration config = buildPoolConfig();
        InferContextLease<Object> lease = InferContextPool.borrow(config);
        assertThrows(RuntimeException.class, () -> lease.infer("value"));
        lease.close();

        try (InferContextLease<Object> next = InferContextPool.borrow(config)) {
            assertEquals(next.infer("value"), Integer.valueOf(2));
        }

        verify(brokenContext, times(1)).close();
    }

    @Test
    public void testContextReturnsToPoolAfterTransientInferFailure() throws Exception {
        AtomicInteger created = new AtomicInteger();
        AtomicInteger invocations = new AtomicInteger();
        InferContext<Object> inferContext = mock(InferContext.class);
        doAnswer(invocation -> {
            if (invocations.getAndIncrement() == 0) {
                throw new RuntimeException("temporary failure");
            }
            return 7;
        }).when(inferContext).infer(org.mockito.Matchers.anyVararg());
        doAnswer(invocation -> false).when(inferContext).isBroken();
        InferContextPool.setContextFactoryForTest(config -> {
            created.incrementAndGet();
            return inferContext;
        });

        Configuration config = buildPoolConfig();
        try (InferContextLease<Object> lease = InferContextPool.borrow(config)) {
            assertThrows(RuntimeException.class, () -> lease.infer("value"));
        }

        try (InferContextLease<Object> retry = InferContextPool.borrow(config)) {
            assertEquals(retry.infer("value"), Integer.valueOf(7));
        }

        assertEquals(created.get(), 1);
        verify(inferContext, times(0)).close();
    }

    @Test
    public void testCloseAllClosesBorrowedAndIdleContexts() throws Exception {
        AtomicInteger created = new AtomicInteger();
        InferContext<Object> firstContext = mockContext(1);
        InferContext<Object> secondContext = mockContext(2);
        InferContextPool.setContextFactoryForTest(config ->
            created.getAndIncrement() == 0 ? firstContext : secondContext);

        Configuration config = buildPoolConfig();
        InferContextLease<Object> borrowedLease = InferContextPool.borrow(config);
        try (InferContextLease<Object> idleLease = InferContextPool.borrow(config)) {
            assertEquals(idleLease.infer("value"), Integer.valueOf(2));
        }

        InferContextPool.closeAll();
        borrowedLease.close();

        verify(firstContext, times(1)).close();
        verify(secondContext, times(1)).close();
    }

    private InferContext<Object> mockContext(Object response) {
        InferContext<Object> inferContext = mock(InferContext.class);
        try {
            doAnswer(invocation -> response).when(inferContext).infer(org.mockito.Matchers.anyVararg());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        doAnswer(invocation -> false).when(inferContext).isBroken();
        return inferContext;
    }

    private Configuration buildPoolConfig() {
        Configuration config = new Configuration();
        config.setMasterId("master");
        config.put("k1", "v1");
        config.put("k2", "v2");
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE, "2");
        config.put(FrameworkConfigKeys.INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC, "1");
        config.put(FrameworkConfigKeys.INFER_ENV_SHARE_MEMORY_QUEUE_SIZE, "1024");
        config.put(ExecutionConfigKeys.JOB_WORK_PATH, System.getProperty("java.io.tmpdir"));
        config.put(FileConfigKeys.USER_NAME, "tester");
        config.put(ExecutionConfigKeys.JOB_UNIQUE_ID, "job");
        return config;
    }
}
