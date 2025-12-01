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

package org.apache.geaflow.context.core.retriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;

/**
 * 关键词检索器 - 简单的字符串匹配检索
 * 
 * <p>对实体名称进行简单的包含匹配，适用于精确关键词查找
 */
public class KeywordRetriever implements Retriever {

    private Map<String, Episode.Entity> entityStore;

    public KeywordRetriever(Map<String, Episode.Entity> entityStore) {
        this.entityStore = entityStore;
    }

    public void setEntityStore(Map<String, Episode.Entity> entityStore) {
        this.entityStore = entityStore;
    }

    @Override
    public List<RetrievalResult> retrieve(ContextQuery query, int topK) throws Exception {
        List<RetrievalResult> results = new ArrayList<>();
        
        if (query.getQueryText() == null || query.getQueryText().isEmpty()) {
            return results;
        }
        
        String queryText = query.getQueryText().toLowerCase();
        
        for (Map.Entry<String, Episode.Entity> entry : entityStore.entrySet()) {
            Episode.Entity entity = entry.getValue();
            if (entity.getName() != null && entity.getName().toLowerCase().contains(queryText)) {
                // 简单评分：完全匹配1.0，部分匹配0.5
                double score = entity.getName().equalsIgnoreCase(queryText) ? 1.0 : 0.5;
                results.add(new RetrievalResult(entity.getId(), score, entity));
            }
            
            if (results.size() >= topK) {
                break;
            }
        }
        
        return results;
    }

    @Override
    public String getName() {
        return "Keyword";
    }

    @Override
    public boolean isAvailable() {
        return entityStore != null && !entityStore.isEmpty();
    }
}
