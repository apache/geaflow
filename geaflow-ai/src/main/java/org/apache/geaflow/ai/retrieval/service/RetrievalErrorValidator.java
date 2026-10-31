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

package org.apache.geaflow.ai.retrieval.service;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalError;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;

/** Validates stable machine-readable error responses. */
public final class RetrievalErrorValidator {

    private RetrievalErrorValidator() {
    }

    public static RetrievalError validate(RetrievalError error) {
        if (error == null || blank(error.getRequestId()) || error.getCode() == null
            || blank(error.getMessage())) {
            throw invalid("requestId, code, and message are required");
        }
        if (error.isRetriable() != error.getCode().isRetriable()) {
            throw invalid("retriable does not match error code");
        }
        return error;
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, message);
    }
}
