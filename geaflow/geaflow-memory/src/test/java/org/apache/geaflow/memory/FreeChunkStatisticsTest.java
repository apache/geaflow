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

package org.apache.geaflow.memory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FreeChunkStatisticsTest extends MemoryReleaseTest {

    @Test
    public void test() throws InterruptedException {

        FreeChunkStatistics statistics = new FreeChunkStatistics(10, 1000);

        statistics.update(1);

        statistics.update(2);

        Assert.assertFalse(statistics.isFull());

        Assert.assertEquals(statistics.getMinFree(), 1);

        for (int i = 0; i < 10; i++) {

            Thread.sleep(1001);

            statistics.update(i);

        }

        Assert.assertEquals(statistics.getMinFree(), 0);

        Assert.assertTrue(statistics.isFull());

        statistics.clear();

        Assert.assertFalse(statistics.isFull());
    }
}
