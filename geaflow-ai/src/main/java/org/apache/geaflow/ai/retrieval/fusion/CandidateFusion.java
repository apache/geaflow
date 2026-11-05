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
package org.apache.geaflow.ai.retrieval.fusion;

/**
 * Design-level name for the candidate fusion strategy.
 *
 * <p>The design document lists {@code CandidateFusion} among the internal extension interfaces. The
 * implementation ships as {@link Fusion}; this interface pins the documented name so the seam is
 * discoverable, and it extends the working contract rather than duplicating it.
 *
 * <p>Fusion is a pure function of the evidence it receives: replacing it must not change the
 * execution mode, the budget accounting or the trace shape.
 */
public interface CandidateFusion extends Fusion {
}