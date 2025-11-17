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
import com.alibaba.fastjson.JSONObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.apache.geaflow.console.core.model.llm.LocalConfigArgsClass;

public class LocalClient extends LLMClient {

    private static final LLMClient INSTANCE = new LocalClient();

    private static final int DEFAULT_N_PREDICT = 128;

    private String getJsonString(LocalConfigArgsClass llm, String prompt) {
        Integer predict = llm.getPredict();
        int nPredict = (predict != null) ? predict : DEFAULT_N_PREDICT;
        JSONObject root = new JSONObject();
        root.put("prompt", prompt.trim());
        root.put("n_predict", nPredict);
        return root.toJSONString();
    }

    private LocalClient() {

    }

    public static LLMClient getInstance() {
        return INSTANCE;
    }

    @Override
    protected Response sendRequest(GeaflowLLM llm, String prompt) {
        try {
            LocalConfigArgsClass config = getConfig(llm, LocalConfigArgsClass.class);
            String jsonString = getJsonString(config, prompt);
            OkHttpClient client = getHttpClient(config);
            MediaType type = MediaType.get("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(jsonString, type);

            Request request = new Request.Builder()
                .url(llm.getUrl())
                .addHeader("Content-Type", "application/json")
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
            JSONObject error = jsonObject.getJSONObject("error");
            if (error != null) {
                return error.getString("message");
            }
            return jsonObject.getString("content");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
