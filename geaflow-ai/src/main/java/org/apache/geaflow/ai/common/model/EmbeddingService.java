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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class EmbeddingService extends AbstractModelService {

    public EmbeddingService() {
        super(new ModelConfig());
    }

    public EmbeddingService(String model) {
        super(new ModelConfig());
        getModelConfig().setModel(model);
    }

    public EmbeddingService(ModelConfig modelConfig) {
        super(modelConfig);
    }

    public String embedding(String... inputs) {
        RemoteModelClient model = new RemoteModelClient();
        ModelEmbedding context = ModelEmbedding.embedding(getModelConfig(), inputs);
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

    public static List<double[]> getVec(String input) {
        Gson gson = new Gson();
        List<String> jsonArray = Arrays.stream(input.split("\n")).filter(
            StringUtils::isNoneBlank).collect(Collectors.toList());
        List<double[]> res = new ArrayList<>(jsonArray.size());
        for (String json : jsonArray) {
            res.add(gson.fromJson(json, EmbeddingResult.class).embedding);
        }
        return res;
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
