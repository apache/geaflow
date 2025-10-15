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

package org.apache.geaflow.console.core.service.llm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Optional;
import java.util.function.Supplier;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.apache.geaflow.console.core.model.llm.OpenAIConfigArgsClass;

public class OpenAiClient extends LLMClient {

    private static final LLMClient INSTANCE = new OpenAiClient();

    private static final String TEMPLATE = "{"
        + "\"model\":\"%s\","
        + "\"messages\": [{\"role\": \"user\", \"content\": \"%s\"}]}";

    private String getJsonString(OpenAIConfigArgsClass config, String prompt) {
        return String.format(TEMPLATE, config.getModelId(), prompt.trim());
    }

    private OpenAiClient() {

    }

    public static LLMClient getInstance() {
        return INSTANCE;
    }

    @Override
    protected Response sendRequest(GeaflowLLM llm, String prompt) {
        try {

            OpenAIConfigArgsClass config = getConfig(llm, OpenAIConfigArgsClass.class);

            String jsonString = getJsonString(config, prompt);
            OkHttpClient client = getHttpClient(config);
            MediaType type = MediaType.get("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(jsonString, type);

            Request request = new Request.Builder()
                .url(llm.getUrl())
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Bearer " + config.getApiKey())
                .addHeader("Cache-Control", "no-cache")
                .post(body)
                .build();

            Response response = client.newCall(request).execute();
            return response;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String parseResult(Response response) {
        try {
            String res = response.body().string();
            JSONObject jsonObject = JSON.parseObject(res);

            JSONArray choices = jsonObject.getJSONArray("choices");
            if (choices == null) {
                return Optional.ofNullable(jsonObject.getJSONObject("error"))
                    .map(e -> e.getString("message"))
                    .orElseThrow((Supplier<Throwable>) () -> new GeaflowException("request failed"));
            }

            JSONObject choice = choices.getJSONObject(0);
            if (!choice.getString("finish_reason").equals("stop")) {
                throw new GeaflowException("request failed");
            }
            return choice
                .getJSONObject("message")
                .getString("content");

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
