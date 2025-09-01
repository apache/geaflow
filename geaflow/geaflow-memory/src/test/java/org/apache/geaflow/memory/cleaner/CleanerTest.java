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

package org.apache.geaflow.memory.cleaner;

import java.nio.ByteBuffer;
import org.apache.geaflow.memory.DirectMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CleanerTest {

    @Test
    public void test() {
        ByteBuffer bf = DirectMemory.allocateDirectNoCleaner(20);

        if (DirectMemory.javaVersion() < 9) {
            Assert.assertTrue(CleanerJava6.isSupported());
            Assert.assertFalse(CleanerJava9.isSupported());
            CleanerJava6 cleanerJava = new CleanerJava6();
            cleanerJava.freeDirectBuffer(bf);
        } else {
            Assert.assertFalse(CleanerJava6.isSupported());
            Assert.assertTrue(CleanerJava9.isSupported());

            CleanerJava9 cleanerJava = new CleanerJava9();
            cleanerJava.freeDirectBuffer(bf);
        }
    }
}
