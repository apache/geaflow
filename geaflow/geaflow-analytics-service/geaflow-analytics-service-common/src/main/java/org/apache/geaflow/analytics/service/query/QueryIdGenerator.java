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

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Chars;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/* This file is based on source code from the Presto Project (https://prestodb.github.io/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Presto's com.facebook.presto.execution.QueryIdGenerator.
 */
public class QueryIdGenerator {

    private static final char[] BASE_32 = {
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '2', '3', '4', '5', '6', '7', '8', '9'};

    static {
        checkState(ImmutableSet.copyOf(Chars.asList(BASE_32)).size() == 32);
    }

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormat.forPattern(
        "YYYYMMdd_HHmmss").withZoneUTC();

    private static final long BASE_SYSTEM_TIME_MILLIS = System.currentTimeMillis();
    private static final long BASE_NANO_TIME = System.nanoTime();
    private static final int COUNT_LIMIT = 99_999;
    private static final int ID_SIZE = 5;
    private final String coordinatorId;
    private long lastTimeInDays;

    private long lastTimeInSeconds;

    private String lastTimestamp;

    private int counter;

    public QueryIdGenerator() {
        StringBuilder coordinatorId = new StringBuilder(ID_SIZE);
        for (int i = 0; i < ID_SIZE; i++) {
            coordinatorId.append(BASE_32[ThreadLocalRandom.current().nextInt(BASE_32.length)]);
        }
        this.coordinatorId = coordinatorId.toString();
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public synchronized String createQueryId() {
        if (counter > COUNT_LIMIT) {
            while (MILLISECONDS.toSeconds(nowInMillis()) == lastTimeInSeconds) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            counter = 0;
        }

        long now = nowInMillis();
        if (MILLISECONDS.toSeconds(now) != lastTimeInSeconds) {
            lastTimeInSeconds = MILLISECONDS.toSeconds(now);
            lastTimestamp = TIMESTAMP_FORMAT.print(now);
            if (MILLISECONDS.toDays(now) != lastTimeInDays) {
                lastTimeInDays = MILLISECONDS.toDays(now);
                counter = 0;
            }
        }
        return String.format("%s_%05d_%s", lastTimestamp, counter++, coordinatorId);
    }

    @VisibleForTesting
    protected long nowInMillis() {
        return BASE_SYSTEM_TIME_MILLIS + TimeUnit.NANOSECONDS.toMillis(
            System.nanoTime() - BASE_NANO_TIME);
    }
}
