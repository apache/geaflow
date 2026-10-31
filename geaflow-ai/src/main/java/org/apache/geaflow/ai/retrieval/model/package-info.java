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
 * Immutable value objects shared by GraphRAG ingestion, retrieval, and evaluation.
 *
 * <p>IDs in these objects are opaque values supplied by callers. This package does not generate
 * IDs. Use {@code sameIdentityAs} for identity comparisons and {@code equals} when complete value
 * equality is required. Evidence identity uses nested references when present, and falls back to
 * evidenceId or text when no references are available; scores and ranks are ignored.</p>
 *
 * <p>JSON uses explicit camelCase field names. Optional fields may be added only additively; unknown
 * fields are ignored, null optional scalars are omitted, and collection fields use empty arrays or
 * objects. Callers must use {@link RetrievalModelJson} for deserialization so required-field and
 * range validation cannot be bypassed by reflection. Text offsets are half-open
 * ({@code startOffset <= endOffset}); ranks are one-based, raw scores are finite, and normalized
 * and fused scores are finite values in {@code [0, 1]}.</p>
 *
 * <p>Identity fields are documentId/dataset/datasetVersion/split/sourceUri/sourceHash for
 * {@code SourceDocument}; chunkId/documentId/chunkIndex/startOffset/endOffset/text/policyVersion/
 * textHash for {@code TextChunk}; entityId/canonicalName/type for {@code EntityRef}; all fields
 * for {@code GraphVertexRef}, {@code GraphVersion}, {@code IndexVersion}, {@code SourceRef},
 * {@code GraphPathRef}, and {@code ChannelScore}; and edgeId/label/sourceEntityId/targetEntityId
 * for {@code GraphEdgeRef}. Evidence compares kind and nested reference identities. Collection
 * order does not affect Evidence identity, while vertex and edge order inside one graph path does.
 * A graph path has exactly {@code hop + 1} vertices and {@code hop} edges. Evidence kind labels
 * the candidate payload (chunk, entity, path, or graph fact), while optional lists preserve the
 * supporting provenance and may be empty for compatibility.</p>
 */
package org.apache.geaflow.ai.retrieval.model;

import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
