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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.Objects;

/** One auditable correction applied to a routing plan before execution. */
public final class PlanAdjustment {
    public static final String DROPPED_UNAVAILABLE_CHANNEL = "DROPPED_UNAVAILABLE_CHANNEL";
    public static final String DOWNGRADED_HYBRID_MODE = "DOWNGRADED_HYBRID_MODE";
    public static final String CLAMPED_MAX_HOP = "CLAMPED_MAX_HOP";
    public static final String CLAMPED_NEIGHBOR_CAP = "CLAMPED_NEIGHBOR_CAP";
    public static final String CLAMPED_CHANNEL_BUDGET = "CLAMPED_CHANNEL_BUDGET";
    public static final String CLAMPED_HARD_UPPER_BOUND = "CLAMPED_HARD_UPPER_BOUND";
    public static final String CLAMPED_TOP_K = "CLAMPED_TOP_K";

    private final String field;
    private final String from;
    private final String to;
    private final String reason;

    public PlanAdjustment(String field, Object from, Object to, String reason) {
        this.field = requireText(field, "field");
        this.from = String.valueOf(from);
        this.to = String.valueOf(to);
        this.reason = requireText(reason, "reason");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    public String getField() {
        return field;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return field + ":" + from + "->" + to + "(" + reason + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PlanAdjustment)) {
            return false;
        }
        PlanAdjustment that = (PlanAdjustment) other;
        return field.equals(that.field) && from.equals(that.from) && to.equals(that.to)
            && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, from, to, reason);
    }
}