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

import java.util.ArrayList;
import java.util.List;

public class OfflineModelDirect {


    public String chat(ModelContext context) {
        ModelInfo info = context.getModelInfo();
        OkHttpDirectConnector connector = new OkHttpDirectConnector(
                info.getUrl(), info.getApi(), info.getUserToken());
        String request = new Gson().toJson(context);
        org.apache.geaflow.ai.common.model.Response response = connector.post(request);
        if (response.choices != null && !response.choices.isEmpty()) {
            for (Response.Choice choice : response.choices) {
                if (choice.message != null) {
                    return choice.message.content;
                }
            }

        }
        return null;
    }

    public List<ChatRobot.EmbeddingResult> embedding(ModelEmbedding context) {
        ModelInfo info = context.getModelInfo();
        OkHttpDirectConnector connector = new OkHttpDirectConnector(
                info.getUrl(), info.getApi(), info.getUserToken());
        String request = new Gson().toJson(context);
        EmbeddingResponse response = connector.embeddingPost(request);
        List<ChatRobot.EmbeddingResult> embeddingResults = new ArrayList<>();
        for (EmbeddingResponse.EmbeddingVector v : response.data) {
            int index = v.index;
            String input = context.input[index];
            double[] vector = v.embedding;
            ChatRobot.EmbeddingResult result = new ChatRobot.EmbeddingResult(input, vector);
            embeddingResults.add(result);
        }
        return embeddingResults;
    }
}
