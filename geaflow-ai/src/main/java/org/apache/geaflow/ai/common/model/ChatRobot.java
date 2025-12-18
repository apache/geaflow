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
import java.util.List;

public class ChatRobot {

    private ModelInfo modelInfo;

    public ChatRobot() {
        this.modelInfo = new ModelInfo();
    }

    public ChatRobot(String model) {
        this.modelInfo = new ModelInfo();
        this.modelInfo.setModel(model);
    }

    public String singleSentence(String sentence) {
        OfflineModelDirect model = new OfflineModelDirect();
        ModelContext context = ModelContext.emptyContext();
        context.setModelInfo(modelInfo);
        context.userSay(sentence);
        return model.chat(context);
    }

    public String embedding(String... inputs) {
        OfflineModelDirect model = new OfflineModelDirect();
        ModelEmbedding context = ModelEmbedding.embedding(modelInfo, inputs);
        List<EmbeddingResult> embeddingResults = model.embedding(context);
        Gson gson = new Gson();
        StringBuilder builder = new StringBuilder();
        for (EmbeddingResult result : embeddingResults) {
            builder.append("\n");
            String json = gson.toJson(result);
            builder.append(json);
        }
        return builder.toString();
    }

    public EmbeddingResult embeddingSingle(String input) {
        OfflineModelDirect model = new OfflineModelDirect();
        ModelEmbedding context = ModelEmbedding.embedding(modelInfo, input);
        List<EmbeddingResult> embeddingResults = model.embedding(context);
        return embeddingResults.get(0);
    }

    public ModelInfo getModelInfo() {
        return modelInfo;
    }

    public void setModelInfo(ModelInfo modelInfo) {
        this.modelInfo = modelInfo;
    }

    public static class EmbeddingResult {
        public String input;
        public double[] embedding;

        public EmbeddingResult(String input, double[] embedding) {
            this.input = input;
            this.embedding = embedding;
        }
    }
}
