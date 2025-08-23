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
package org.apache.geaflow.analytics.service.query;

import static org.testng.Assert.assertEquals;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

public class QueryIdGeneratorTest {

    @Test
    public void testCreateNextQueryId() {
        QueryIdGeneratorOnlyTest queryIdGenerator = new QueryIdGeneratorOnlyTest();
        long currentMillis = new DateTime(2023, 8, 12, 1, 2, 3, 4, DateTimeZone.UTC).getMillis();
        queryIdGenerator.setCurrentTime(currentMillis);

        // Generate ids to 99,999
        for (int i = 0; i < 100_000; i++) {
            assertEquals(queryIdGenerator.createQueryId(), String.format("20230812_010203_%05d_%s", i, queryIdGenerator.getCoordinatorId()));
        }

        currentMillis += 1000;
        queryIdGenerator.setCurrentTime(currentMillis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(queryIdGenerator.createQueryId(), String.format("20230812_010204_%05d_%s", i, queryIdGenerator.getCoordinatorId()));
        }

        currentMillis += 1000;
        queryIdGenerator.setCurrentTime(currentMillis);
        for (int i = 0; i < 100; i++) {
            assertEquals(queryIdGenerator.createQueryId(), String.format("20230812_010205_%05d_%s", i, queryIdGenerator.getCoordinatorId()));
        }

        currentMillis = new DateTime(2023, 8, 13, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        queryIdGenerator.setCurrentTime(currentMillis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(queryIdGenerator.createQueryId(), String.format("20230813_000000_%05d_%s", i, queryIdGenerator.getCoordinatorId()));
        }
    }

    private static class QueryIdGeneratorOnlyTest extends QueryIdGenerator {

        private long currentTime;

        public void setCurrentTime(long currentTime) {
            this.currentTime = currentTime;
        }

        @Override
        protected long nowInMillis() {
            return currentTime;
        }
    }
}
