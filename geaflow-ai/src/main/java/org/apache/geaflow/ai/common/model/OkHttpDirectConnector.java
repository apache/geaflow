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

package org.apache.geaflow.ai.common.model;

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

public class OkHttpDirectConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpDirectConnector.class);

    private static final Gson GSON = new Gson();
    private static OkHttpClient client;

    private final String endpoint;
    private final String useApi;
    private final String userToken;

    public OkHttpDirectConnector(String endpoint, String useApi, String userToken) {
        this.endpoint = endpoint;
        this.useApi = useApi;
        this.userToken = userToken;
        if (client == null) {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.callTimeout(Constants.HTTP_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.connectTimeout(Constants.HTTP_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.readTimeout(Constants.HTTP_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.writeTimeout(Constants.HTTP_WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client = builder.build();
        }
    }

    public org.apache.geaflow.ai.common.model.Response post(String bodyJson) {
        RequestBody requestBody = RequestBody.create(
                MediaType.parse("application/json; charset=utf-8"),
                bodyJson
        );

        String url = endpoint + useApi;
        LOGGER.info(url);
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + userToken)
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .post(requestBody)
                .build();

        try (okhttp3.Response response = client.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                String responseBody = response.body().string();
                return GSON.fromJson(responseBody, org.apache.geaflow.ai.common.model.Response.class);
            } else {
                LOGGER.info("Request failed with code: " + response.code());
            }
        } catch (IOException e) {
            LOGGER.error("Http connect exception", e);
            throw new RuntimeException(e);
        }
        return null;
    }


    public EmbeddingResponse embeddingPost(String bodyJson) {
        RequestBody requestBody = RequestBody.create(
                MediaType.parse("application/json; charset=utf-8"),
                bodyJson
        );

        String url = endpoint + useApi;
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + userToken)
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .post(requestBody)
                .build();

        try (okhttp3.Response response = client.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                String responseBody = response.body().string();
                return GSON.fromJson(responseBody, EmbeddingResponse.class);
            } else {
                LOGGER.info("Request failed with code: " + response.code());
                LOGGER.info("Request failed with request bodyJson: "
                        + bodyJson);
                LOGGER.info("Request failed with response body: "
                        + Objects.requireNonNull(response.body()).string());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
