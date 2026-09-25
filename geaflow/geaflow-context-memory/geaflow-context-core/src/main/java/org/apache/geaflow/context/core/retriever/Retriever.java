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

import java.util.List;
import org.apache.geaflow.context.api.query.ContextQuery;

/**
 * 检索器抽象接口
 * 
 * <p>定义了统一的检索接口，支持多种检索策略的实现。
 * 所有检索器都返回统一的{@link RetrievalResult}，便于后续融合。
 */
public interface Retriever {

    /**
     * 检索方法
     * 
     * @param query 查询对象
     * @param topK 返回top K结果
     * @return 检索结果列表（按相关性降序）
     * @throws Exception 如果检索失败
     */
    List<RetrievalResult> retrieve(ContextQuery query, int topK) throws Exception;

    /**
     * 获取检索器名称（用于日志和融合标识）
     */
    String getName();

    /**
     * 检索器是否已初始化并可用
     */
    boolean isAvailable();

    /**
     * 检索结果统一封装
     */
    class RetrievalResult implements Comparable<RetrievalResult> {
        private final String entityId;
        private final double score;
        private final Object metadata;  // 可选的元数据

        public RetrievalResult(String entityId, double score) {
            this(entityId, score, null);
        }

        public RetrievalResult(String entityId, double score, Object metadata) {
            this.entityId = entityId;
            this.score = score;
            this.metadata = metadata;
        }

        public String getEntityId() {
            return entityId;
        }

        public double getScore() {
            return score;
        }

        public Object getMetadata() {
            return metadata;
        }

        @Override
        public int compareTo(RetrievalResult other) {
            return Double.compare(other.score, this.score);  // 降序
        }

        @Override
        public String toString() {
            return String.format("RetrievalResult{id='%s', score=%.4f}", entityId, score);
        }
    }
}
