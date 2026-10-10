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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api.model;

/** Stable machine-readable retrieval failure codes and HTTP semantics. */
public enum RetrievalErrorCode {
    INVALID_REQUEST(400, false),
    UNSUPPORTED_OPTION(400, false),
    AUTHENTICATION_REQUIRED(401, false),
    FORBIDDEN(403, false),
    GRAPH_NOT_FOUND(404, false),
    INDEX_NOT_READY(503, true),
    RETRIEVAL_TIMEOUT(504, true),
    INTERNAL_ERROR(500, false);

    private final int httpStatus;
    private final boolean retriable;

    RetrievalErrorCode(int httpStatus, boolean retriable) {
        this.httpStatus = httpStatus;
        this.retriable = retriable;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public boolean isRetriable() {
        return retriable;
    }
}
