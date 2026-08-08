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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.geaflow.infer.exchange.DataExchangeContext;
import org.apache.geaflow.infer.exchange.UnSafeUtils;
import org.apache.geaflow.infer.exchange.impl.InferDataBridgeImpl;
import org.testng.annotations.Test;

public class InferContextCloseTest {

    @Test
    public void testCloseDoesNotWaitForPendingInferRead() throws Exception {
        InferContext<Object> context = newBareContext();
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicReference<Object> inferResult = new AtomicReference<>();
        AtomicReference<Throwable> inferError = new AtomicReference<>();

        InferDataBridgeImpl<Object> dataBridge = mock(InferDataBridgeImpl.class);
        doNothing().when(dataBridge).close();
        doAnswer(invocation -> true).when(dataBridge).write(org.mockito.Matchers.anyVararg());
        doAnswer(invocation -> {
            readEntered.countDown();
            if (!releaseRead.await(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for close signal");
            }
            return "ok";
        }).when(dataBridge).read();

        DataExchangeContext shareMemoryContext = mock(DataExchangeContext.class);
        doAnswer(invocation -> {
            releaseRead.countDown();
            return null;
        }).when(shareMemoryContext).markFinished();
        doNothing().when(shareMemoryContext).close();

        setField(context, "inferLock", new ReentrantLock());
        setField(context, "closed", new AtomicBoolean(false));
        setField(context, "broken", false);
        setField(context, "dataBridge", dataBridge);
        setField(context, "shareMemoryContext", shareMemoryContext);

        Thread inferThread = new Thread(() -> {
            try {
                inferResult.set(context.infer("payload"));
            } catch (Throwable e) {
                inferError.set(e);
            }
        });
        inferThread.start();

        assertTrue(readEntered.await(1, TimeUnit.SECONDS), "infer() did not enter read()");

        Thread closeThread = new Thread(context::close);
        closeThread.start();
        Thread.sleep(200L);

        assertFalse(closeThread.isAlive(), "close() should not wait for infer() to finish");

        inferThread.join(1000L);
        closeThread.join(1000L);

        assertFalse(inferThread.isAlive(), "infer() did not complete after close()");
        assertFalse(closeThread.isAlive(), "close() did not complete");
        assertEquals(inferResult.get(), "ok");
        assertEquals(inferError.get(), null);
    }

    private InferContext<Object> newBareContext() throws InstantiationException {
        @SuppressWarnings("unchecked")
        InferContext<Object> context = (InferContext<Object>) UnSafeUtils.UNSAFE
            .allocateInstance(InferContext.class);
        return context;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = InferContext.class.getDeclaredField(fieldName);
        long offset = UnSafeUtils.UNSAFE.objectFieldOffset(field);
        if (field.getType() == boolean.class) {
            UnSafeUtils.UNSAFE.putBoolean(target, offset, (Boolean) value);
        } else {
            UnSafeUtils.UNSAFE.putObject(target, offset, value);
        }
    }
}
