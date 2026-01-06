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
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.common.utils.RetryCommand;

public class RemoteModelClient {

    public String chat(ModelContext context) {
        ModelConfig info = context.getModelInfo();
        OkHttpDirectConnector connector = new OkHttpDirectConnector(
                info.getUrl(), info.getApi(), info.getUserToken());
        String request = new Gson().toJson(context);
        org.apache.geaflow.ai.common.model.Response response = RetryCommand.run(() -> {
            return connector.post(request);
        }, Constants.MODEL_CLIENT_RETRY_TIMES, Constants.MODEL_CLIENT_RETRY_INTERVAL_MS);
        if (response.choices != null && !response.choices.isEmpty()) {
            for (Response.Choice choice : response.choices) {
                if (choice.message != null) {
                    return choice.message.content;
                }
            }

        }
        return null;
    }

    public List<EmbeddingService.EmbeddingResult> embedding(ModelEmbedding context) {
        ModelConfig info = context.getModelInfo();
        OkHttpDirectConnector connector = new OkHttpDirectConnector(
                info.getUrl(), info.getApi(), info.getUserToken());
        ModelEmbedding requestContext = new ModelEmbedding(null, context.input);
        requestContext.setModel(context.getModel());
        String request = new Gson().toJson(requestContext);
        final EmbeddingResponse response = RetryCommand.run(() -> {
            return connector.embeddingPost(request);
        }, Constants.MODEL_CLIENT_RETRY_TIMES, Constants.MODEL_CLIENT_RETRY_INTERVAL_MS);
        if (response == null) {
            return new ArrayList<>();
        }
        List<EmbeddingService.EmbeddingResult> embeddingResults = new ArrayList<>();
        if (response.data == null) {
            throw new RuntimeException("Embedding model response is null");
        }
        for (EmbeddingResponse.EmbeddingVector v : response.data) {
            int index = v.index;
            if (index >= context.input.length) {
                throw new RuntimeException("Embedding model response contains invalid index");
            }
            String input = context.input[index];
            double[] vector = v.embedding;
            EmbeddingService.EmbeddingResult result = new EmbeddingService.EmbeddingResult(input, vector);
            embeddingResults.add(result);
        }
        return embeddingResults;
    }
}
