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

package org.apache.geaflow.ai.temporal.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeIntervalTest {

    @Test
    public void testContainsUsesHalfOpenBounds() {
        TimeInterval interval = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");

        Assertions.assertTrue(interval.contains(time("2024-01-01T00:00:00Z")));
        Assertions.assertTrue(interval.contains(time("2024-06-01T00:00:00Z")));
        Assertions.assertFalse(interval.contains(time("2025-01-01T00:00:00Z")));
        Assertions.assertFalse(interval.contains(time("2023-12-31T23:59:59Z")));
    }

    @Test
    public void testUnboundedInterval() {
        TimeInterval interval =
            TimeInterval.unboundedFrom(time("2024-01-01T00:00:00Z"));

        Assertions.assertFalse(interval.getEnd().isPresent());
        Assertions.assertTrue(interval.contains(time("2099-01-01T00:00:00Z")));
        Assertions.assertFalse(interval.contains(time("2023-01-01T00:00:00Z")));
    }

    @Test
    public void testIntersectionAndOverlap() {
        TimeInterval left = interval(
            "2024-01-01T00:00:00Z",
            "2024-10-01T00:00:00Z");
        TimeInterval right = interval(
            "2024-05-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        TimeInterval touching = interval(
            "2024-10-01T00:00:00Z",
            "2025-01-01T00:00:00Z");

        Assertions.assertTrue(left.overlaps(right));
        Assertions.assertEquals(
            interval("2024-05-01T00:00:00Z", "2024-10-01T00:00:00Z"),
            left.intersection(right).get());

        Assertions.assertFalse(left.overlaps(touching));
        Assertions.assertFalse(left.intersection(touching).isPresent());
    }

    @Test
    public void testSubtract() {
        TimeInterval whole = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        TimeInterval middle = interval(
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z");

        Assertions.assertEquals(
            Arrays.asList(
                interval("2024-01-01T00:00:00Z", "2024-04-01T00:00:00Z"),
                interval("2024-09-01T00:00:00Z", "2025-01-01T00:00:00Z")),
            whole.subtract(middle));

        Assertions.assertEquals(
            Collections.singletonList(whole),
            whole.subtract(interval(
                "2025-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z")));

        Assertions.assertTrue(whole.subtract(whole).isEmpty());
    }

    @Test
    public void testRejectInvalidInterval() {
        Instant start = time("2024-01-01T00:00:00Z");

        Assertions.assertThrows(
            NullPointerException.class,
            () -> new TimeInterval(null, start));

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TimeInterval(start, start));

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new TimeInterval(
                time("2025-01-01T00:00:00Z"),
                time("2024-01-01T00:00:00Z")));
    }

    @Test
    public void testSubtractFromUnboundedInterval() {
        TimeInterval whole =
            TimeInterval.unboundedFrom(time("2024-01-01T00:00:00Z"));
        TimeInterval removed = interval(
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z");

        Assertions.assertEquals(
            Arrays.asList(
                interval("2024-01-01T00:00:00Z", "2024-04-01T00:00:00Z"),
                TimeInterval.unboundedFrom(time("2024-09-01T00:00:00Z"))),
            whole.subtract(removed));
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
