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

package org.apache.geaflow.cluster.rpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class RpcEndpointRefTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcEndpointRefTest.class);

    @Test
    public void testFuture() throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        SettableFuture future = SettableFuture.create();

        AtomicInteger result = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        handleFutureCallback(future, new RpcEndpointRef.RpcCallback<Object>() {
            @Override
            public void onSuccess(Object value) {
                LOGGER.info("on success");
                result.set(1);
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("on failure");
                result.set(2);
                countDownLatch.countDown();
            }
        }, executorService);

        future.set("test");

        countDownLatch.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(1, result.get());
        Assert.assertEquals("test", future.get());

        System.out.println("final result" + future.get());
    }

    public <T> void handleFutureCallback(ListenableFuture<T> future,
                                         RpcEndpointRef.RpcCallback<T> listener,
                                         ExecutorService executorService) {
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable T result) {
                listener.onSuccess(result);
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("rpc call failed " + t);
                listener.onFailure(t);
            }
        }, executorService);
    }
}