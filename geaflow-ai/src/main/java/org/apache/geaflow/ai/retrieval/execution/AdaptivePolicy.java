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
 */
package org.apache.geaflow.ai.retrieval.execution;

import java.util.Collections;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/**
 * Design-level name for the pluggable routing policy.
 *
 * <p>A policy exposes initial planning and optional stage-boundary replanning. It stays independent
 * from retrieval execution, budget consumption and trace writing.
 *
 * <p>A policy stays a pure decision component: it must not call retrievers, mutate indexes, consume
 * budget or write traces. The executor validates whatever plan it returns against hard budgets,
 * available indexes and supported hop limits before executing it.
 */
public interface AdaptivePolicy {

    /**
     * Returns the initial plan for one request.
     *
     * @param context immutable snapshot of features, probe results, indexes, statistics and budget
     */
    RetrievalPlan plan(RoutingContext context);

    /**
     * Returns a bounded revision of the plan at a configured stage boundary.
     *
     * <p>The default keeps the initial decision, which is what a policy that does not replan should
     * do. A policy must never interrupt a candidate operation already in progress.
     */
    default RetrievalPlan replan(RoutingContext context, RetrievalProgress progress) {
        return plan(context);
    }

    /** Stable identifier recorded in the trace so a routing decision can be replayed. */
    String getPolicyId();

    /** Policy version, recorded alongside the identifier. */
    String getPolicyVersion();

    /** Policy parameters, recorded so a historical decision can be reproduced exactly. */
    default Map<String, String> getParameters() {
        return Collections.emptyMap();
    }

    /** Returns true when this policy can route the requested mode. */
    default boolean supports(RetrievalMode mode) {
        return mode != null;
    }
}
