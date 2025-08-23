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
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.llm.CodefuseConfigArgsClass;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;

public class CodefuseClient extends LLMClient {

    private static final LLMClient INSTANCE = new CodefuseClient();

    private static final String TEMPLATE = "{\n"
        + "    \"sceneName\": \"%s\",\n"
        + "    \"chainName\": \"%s\",\n"
        + "    \"itemId\": \"gpt\",\n"
        + "    \"modelEnv\": \"pre\",\n"
        + "    \"feature\": {\n"
        + "        \"data\": \"{\\\"api_version\\\":\\\"v2\\\",\\\"out_seq_length\\\":300,"
        + "\\\"prompts\\\":[{\\\"prompt\\\":[{\\\"content\\\":\\\"%s\\\",\\\"role\\\":\\\"<human>\\\"}],"
        + "\\\"repetition_penalty\\\":1.1,\\\"temperature\\\":0.2,\\\"top_k\\\":40,\\\"top_p\\\":0.9}],\\\"stream\\\":false}\"\n"
        + "    }\n"
        + "}";

    private String getJsonString(CodefuseConfigArgsClass config, String prompt) {

        String sceneName = config.getSceneName();
        String chainName = config.getChainName();
        return String.format(TEMPLATE, sceneName, chainName, prompt);
    }

    private CodefuseClient() {

    }

    public static LLMClient getInstance() {
        return INSTANCE;
    }

    @Override
    protected Response sendRequest(GeaflowLLM llm, String prompt) {
        try {
            CodefuseConfigArgsClass config = getConfig(llm, CodefuseConfigArgsClass.class);
            String jsonString = getJsonString(config, prompt);
            OkHttpClient client = getHttpClient(config);
            MediaType type = MediaType.get("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(jsonString, type);

            Request request = new Request.Builder()
                .url(llm.getUrl())
                .addHeader("Content-Type", "application/json")
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
            if (!jsonObject.getBooleanValue("success")) {
                throw new GeaflowException("request failed msg:{}", jsonObject.getString("resultMsg"));
            }
            JSONObject o = (JSONObject) jsonObject.getJSONObject("data").getJSONArray("items").get(0);
            JSONArray array = o.getJSONObject("attributes").getJSONObject("res").getJSONArray("generated_code").getJSONArray(0);
            return array.get(0).toString();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
