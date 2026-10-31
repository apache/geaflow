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

package org.apache.geaflow.ai.retrieval.support;

import okhttp3.Headers;

/** Detached HTTP result, safe to inspect after the response has closed. */
public final class HttpTestResponse {

    private final int status;
    private final Headers headers;
    private final String body;

    public HttpTestResponse(int status, Headers headers, String body) {
        this.status = status;
        this.headers = headers;
        this.body = body;
    }

    public int getStatus() {
        return status;
    }

    public int getStatusCode() {
        return status;
    }

    public Headers getHeaders() {
        return headers;
    }

    public String getBody() {
        return body;
    }

    public String getContentType() {
        return headers.get("Content-Type");
    }
}
