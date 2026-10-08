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

/**
 * Versioned retrieval API contract for Java and HTTP clients.
 *
 * <p>The core version {@code v1} facade supports {@code BM25_ONLY}, {@code VECTOR_ONLY},
 * {@code GRAPH_ONLY}, and fixed {@code HYBRID} retrieval with {@code SEQUENTIAL} execution.
 * {@code KEYWORD} is retained as an alias for {@code BM25_ONLY}. A non-empty query vector is
 * required by {@code VECTOR_ONLY} and {@code HYBRID}; it is unsupported for the other modes.
 * {@code PARALLEL} and {@code CASCADED} execution are reserved and reported as
 * {@code UNSUPPORTED_OPTION}. The HTTP endpoint currently exposes {@code KEYWORD} only.
 * Unknown JSON fields are accepted for additive wire compatibility; duplicate fields are
 * rejected. Response collections are always JSON arrays. Hybrid responses include selected
 * channels, channel budgets and statuses, evaluated counts, versions, and stop/degradation
 * reasons in the trace.</p>
 *
 * <p>Error codes map to HTTP status as follows: {@code INVALID_REQUEST}=400,
 * {@code UNSUPPORTED_OPTION}=400, {@code GRAPH_NOT_FOUND}=404,
 * {@code INDEX_NOT_READY}=503, {@code RETRIEVAL_TIMEOUT}=504, and
 * {@code INTERNAL_ERROR}=500. Only {@code INDEX_NOT_READY} and {@code RETRIEVAL_TIMEOUT} are
 * retriable.</p>
 */
package org.apache.geaflow.ai.retrieval.api;
