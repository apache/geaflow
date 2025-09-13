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

/**
 * Twitter-like snowflake id generator.
 * Id is composed of:
 * timestamp | containerId | sequence
 * 41        | 16       | 6
 * timestamp - 41 bits (millisecond precision w/ a custom epoch gives us 69 years)
 * containerId - 16 bits - gives us up to 65536 workers
 * sequence number - 6 bits - rolls over every 64 per machine (with protection to avoid
 * rollover in the same ms)
 */
public class IdGenerator {

    /**
     * Start timestamp.(2022-01-01 00:00:00.000)
     */
    private static final long START_EPOCH = 1611158400000L;

    private final long sequenceBits = 6L;
    private final int containerIdBits = 16;

    private final long maxContainerId = -1L ^ (-1L << containerIdBits);
    private final long containerIdShift = sequenceBits;
    private final long timestampLeftShift = sequenceBits + containerIdBits;

    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private long containerId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    /**
     * Id generator.
     * @param containerId (0~65534).
     */
    public IdGenerator(long containerId) {
        if (containerId > maxContainerId || containerId < 0) {
            throw new IllegalArgumentException(
                String.format("worker Id can't be greater than %d or less than 0", maxContainerId));
        }
        this.containerId = containerId;
    }

    /**
     * Generate next Id.
     *
     * @return
     */
    public synchronized long nextId() {
        long timestamp = currentTimeMillis();

        if (timestamp < lastTimestamp) {
            throw new RuntimeException(
                String.format("Clock moved backwards. Refusing to generate id for %d milliseconds",
                    lastTimestamp - timestamp));
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // blocking till next millis second
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - START_EPOCH) << timestampLeftShift)
            | (containerId << containerIdShift)
            | sequence;
    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = currentTimeMillis();
        }
        return timestamp;
    }

    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
