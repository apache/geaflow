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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/** Loopback HTTP client which never retains live response resources. */
public final class HttpTestClient implements AutoCloseable {

    private final String baseUrl;
    private final OkHttpClient client = new OkHttpClient.Builder()
        .callTimeout(10, TimeUnit.SECONDS)
        .build();

    public HttpTestClient(String baseUrl) {
        Objects.requireNonNull(baseUrl, "baseUrl");
        if (baseUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("baseUrl must not be blank");
        }
        this.baseUrl = baseUrl.endsWith("/")
            ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public HttpTestResponse get(String path) {
        return get(path, Collections.emptyMap());
    }

    public HttpTestResponse get(String path, Map<String, String> parameters) {
        return execute(new Request.Builder().url(url(path, parameters)).get().build());
    }

    public HttpTestResponse post(String path, String body, Map<String, String> parameters) {
        return execute(new Request.Builder().url(url(path, parameters)).post(
            RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
                body == null ? "" : body)).build());
    }

    public HttpTestResponse post(String path, String body) {
        return post(path, body, Collections.emptyMap());
    }

    public HttpTestResponse postWithHeaders(String path, String body, Map<String, String> headers) {
        Request.Builder builder = new Request.Builder().url(url(path, Collections.emptyMap())).post(
            RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
                body == null ? "" : body));
        (headers == null ? Collections.<String, String>emptyMap() : headers)
            .forEach(builder::header);
        return execute(builder.build());
    }

    private HttpUrl url(String path, Map<String, String> parameters) {
        Objects.requireNonNull(path, "path");
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        HttpUrl.Builder url = HttpUrl.get(baseUrl + normalizedPath).newBuilder();
        (parameters == null ? Collections.<String, String>emptyMap() : parameters)
            .forEach(url::addQueryParameter);
        return url.build();
    }

    private HttpTestResponse execute(Request request) {
        try (Response response = client.newCall(request).execute()) {
            return new HttpTestResponse(response.code(), response.headers().newBuilder().build(),
                response.body() == null ? "" : response.body().string());
        } catch (IOException e) {
            throw new IllegalStateException(request.method() + " " + request.url(), e);
        }
    }

    @Override
    public void close() {
        client.connectionPool().evictAll();
        client.dispatcher().executorService().shutdown();
    }
}
