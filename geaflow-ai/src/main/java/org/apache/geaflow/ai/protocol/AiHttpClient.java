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

package org.apache.geaflow.ai.protocol;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.geaflow.ai.common.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiHttpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AiHttpClient.class);
    private static final Gson GSON = new Gson();
    private static OkHttpClient CLIENT;

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final String url;
    private final String token;

    public AiHttpClient(String url, String token) {
        this.url = Objects.requireNonNull(url);
        this.token = token == null ? "" : token;
        if (CLIENT == null) {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.callTimeout(Constants.HTTP_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.connectTimeout(Constants.HTTP_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.readTimeout(Constants.HTTP_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.writeTimeout(Constants.HTTP_WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            CLIENT = builder.build();
        }
    }

    public AiResponse execute(AiEnvelope envelope) {
        String bodyJson = GSON.toJson(envelope);
        RequestBody body = RequestBody.create(JSON, bodyJson);
        Request.Builder builder = new Request.Builder().url(url).post(body)
            .addHeader("Content-Type", "application/json; charset=utf-8");
        if (!token.isEmpty()) {
            builder.addHeader("Authorization", "Bearer " + token);
        }
        Request request = builder.build();

        try (okhttp3.Response response = CLIENT.newCall(request).execute()) {
            if (response.body() == null) {
                throw new RuntimeException("AI response body is null");
            }
            String resp = response.body().string();
            if (!response.isSuccessful()) {
                LOGGER.warn("AI request failed url={}, code={}, body={}", url, response.code(), resp);
                throw new RuntimeException("AI request failed with code=" + response.code());
            }
            return GSON.fromJson(resp, AiResponse.class);
        } catch (IOException e) {
            throw new RuntimeException("AI HTTP call failed", e);
        }
    }

    public <T> T executePayload(AiEnvelope envelope, Class<T> payloadType) {
        AiResponse resp = execute(envelope);
        if (!Boolean.TRUE.equals(resp.ok)) {
            String err = resp.error != null ? (resp.error.code + ": " + resp.error.message) : "unknown error";
            throw new RuntimeException("AI returned ok=false: " + err);
        }
        if (resp.payload == null) {
            return null;
        }
        return GSON.fromJson(resp.payload, payloadType);
    }
}

