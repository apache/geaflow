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
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class OkHttpDirectConnector {

    private static Gson GSON = new Gson();

    private String endpoint;
    private String useApi;
    private String userToken;
    private OkHttpClient client;

    public OkHttpDirectConnector(String endpoint, String useApi, String userToken) {
        this.endpoint = endpoint;
        this.useApi = useApi;
        this.userToken = userToken;
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.callTimeout(300, TimeUnit.SECONDS);
        builder.connectTimeout(300, TimeUnit.SECONDS);
        builder.readTimeout(300, TimeUnit.SECONDS);
        builder.writeTimeout(300, TimeUnit.SECONDS);
        this.client = builder.build();
    }

    public org.apache.geaflow.ai.common.model.Response post(String bodyJson) {
        RequestBody requestBody = RequestBody.create(
                MediaType.parse("application/json; charset=utf-8"),
                bodyJson
        );

        String url = endpoint + useApi;
        System.out.println(url);
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
                System.out.println("Request failed with code: " + response.code());
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                System.out.println("Request failed with code: " + response.code());
                System.out.println("Request failed with request bodyJson: " +
                        bodyJson);
                System.out.println("Request failed with response body: " +
                        Objects.requireNonNull(response.body()).string());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
