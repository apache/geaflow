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

package org.apache.geaflow.infer.exchange;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class DataExchangeQueueLifecycleTest {

    @Test
    public void testNewQueueCanStillMarkFinishedAfterPreviousQueueWasClosed() throws Exception {
        DataExchangeQueue previousQueue = newQueue();
        DataExchangeQueue nextQueue = newQueue();
        try {
            previousQueue.close();
            nextQueue.markFinished();

            assertTrue(nextQueue.enableFinished(),
                "markFinished() should be local to the queue instance, not globally disabled");
        } finally {
            freeQueueMemory(previousQueue);
            freeQueueMemory(nextQueue);
        }
    }

    @Test
    public void testMarkFinishedIsNoOpAfterQueueClosed() throws Exception {
        DataExchangeQueue queue = newQueue();
        try {
            queue.close();
            queue.markFinished();

            assertFalse(queue.enableFinished(),
                "markFinished() should be ignored after the queue instance is closed");
        } finally {
            freeQueueMemory(queue);
        }
    }

    @Test
    public void testCloseIsIdempotentPerInstance() throws Exception {
        DataExchangeQueue queue = newQueue();
        try {
            queue.close();
            queue.close();
            assertTrue(((AtomicBoolean) getObjectField(queue, "closed")).get(),
                "close() should mark the queue instance as closed");
        } finally {
            freeQueueMemory(queue);
        }
    }

    private DataExchangeQueue newQueue() throws Exception {
        DataExchangeQueue queue = (DataExchangeQueue) UnSafeUtils.UNSAFE
            .allocateInstance(DataExchangeQueue.class);
        long endPointAddress = UnSafeUtils.UNSAFE.allocateMemory(Long.BYTES);
        UnSafeUtils.UNSAFE.putLong(endPointAddress, 0L);
        setLongField(queue, "endPointAddress", endPointAddress);
        setLongField(queue, "mapAddress", 0L);
        setObjectField(queue, "closed", new AtomicBoolean(false));
        setObjectField(queue, "memoryMapper", null);
        return queue;
    }

    private void freeQueueMemory(DataExchangeQueue queue) throws Exception {
        long endPointAddress = (Long) getObjectField(queue, "endPointAddress");
        if (endPointAddress != 0L) {
            UnSafeUtils.UNSAFE.freeMemory(endPointAddress);
            setLongField(queue, "endPointAddress", 0L);
        }
        long mapAddress = (Long) getObjectField(queue, "mapAddress");
        if (mapAddress != 0L) {
            UnSafeUtils.UNSAFE.freeMemory(mapAddress);
            setLongField(queue, "mapAddress", 0L);
        }
    }

    private void setObjectField(Object target, String fieldName, Object value) throws Exception {
        Field field = DataExchangeQueue.class.getDeclaredField(fieldName);
        long fieldOffset = UnSafeUtils.UNSAFE.objectFieldOffset(field);
        UnSafeUtils.UNSAFE.putObject(target, fieldOffset, value);
    }

    private void setLongField(Object target, String fieldName, long value) throws Exception {
        Field field = DataExchangeQueue.class.getDeclaredField(fieldName);
        long fieldOffset = UnSafeUtils.UNSAFE.objectFieldOffset(field);
        UnSafeUtils.UNSAFE.putLong(target, fieldOffset, value);
    }

    private Object getObjectField(Object target, String fieldName) throws Exception {
        Field field = DataExchangeQueue.class.getDeclaredField(fieldName);
        long fieldOffset = UnSafeUtils.UNSAFE.objectFieldOffset(field);
        if (field.getType() == long.class) {
            return UnSafeUtils.UNSAFE.getLong(target, fieldOffset);
        }
        return UnSafeUtils.UNSAFE.getObject(target, fieldOffset);
    }
}
