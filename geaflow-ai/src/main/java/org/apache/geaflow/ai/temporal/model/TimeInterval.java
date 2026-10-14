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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable half-open time interval.
 *
 * <p>The start is inclusive and the end is exclusive. A null internal end
 * represents positive infinity.
 */
public final class TimeInterval {

    private final Instant start;
    private final Instant end;

    public TimeInterval(Instant start, Instant end) {
        this.start = Objects.requireNonNull(start, "start");
        if (end != null && !start.isBefore(end)) {
            throw new IllegalArgumentException(
                "Interval start must be earlier than end");
        }
        this.end = end;
    }

    public static TimeInterval unboundedFrom(Instant start) {
        return new TimeInterval(start, null);
    }

    public Instant getStart() {
        return start;
    }

    public Optional<Instant> getEnd() {
        return Optional.ofNullable(end);
    }

    public boolean contains(Instant time) {
        Objects.requireNonNull(time, "time");
        return !time.isBefore(start) && (end == null || time.isBefore(end));
    }

    public boolean overlaps(TimeInterval other) {
        return intersection(other).isPresent();
    }

    public Optional<TimeInterval> intersection(TimeInterval other) {
        Objects.requireNonNull(other, "other");

        Instant intersectionStart =
            start.isAfter(other.start) ? start : other.start;
        Instant intersectionEnd = earliestEnd(end, other.end);

        if (intersectionEnd != null
            && !intersectionStart.isBefore(intersectionEnd)) {
            return Optional.empty();
        }
        return Optional.of(
            new TimeInterval(intersectionStart, intersectionEnd));
    }

    public List<TimeInterval> subtract(TimeInterval other) {
        Optional<TimeInterval> intersection = intersection(other);
        if (!intersection.isPresent()) {
            return Collections.singletonList(this);
        }

        TimeInterval overlap = intersection.get();
        List<TimeInterval> remaining = new ArrayList<>(2);

        if (start.isBefore(overlap.start)) {
            remaining.add(new TimeInterval(start, overlap.start));
        }

        if (overlap.end != null
            && (end == null || overlap.end.isBefore(end))) {
            remaining.add(new TimeInterval(overlap.end, end));
        }

        return remaining;
    }

    private static Instant earliestEnd(Instant left, Instant right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return left.isBefore(right) ? left : right;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TimeInterval)) {
            return false;
        }
        TimeInterval that = (TimeInterval) object;
        return start.equals(that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return "[" + start + ", " + (end == null ? "infinity" : end) + ")";
    }
}
