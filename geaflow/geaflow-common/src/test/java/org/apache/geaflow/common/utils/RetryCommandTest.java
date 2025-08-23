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

package org.apache.geaflow.common.utils;

import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RetryCommandTest {

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testFail() {
        final int time = 0;
        RetryCommand.run(() -> 10 / time > 0, 3, 1);
    }

    @Test
    public void testRun() {
        Object result = RetryCommand.run(() -> {
            System.out.println("hello");
            return "null";
        }, 1, 100);
        Assert.assertEquals(result, "null");
    }

    @Test
    public void testException() {
        Object result = null;
        try {
            RetryCommand.run(() -> {
                throw new RuntimeException("exception");
            }, 1, 100);
        } catch (Throwable e) {
            result = e;
        }
        Assert.assertTrue(result instanceof RuntimeException);
    }
}
