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

package org.apache.geaflow.dsl.udf.graph.gcn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;

public class GCNResultDecoder {

    public Row decode(Object vertexId, Object inferResult) {
        if (!(inferResult instanceof Map)) {
            throw new IllegalArgumentException(
                "GCN infer result must be a map: " + (inferResult == null ? "null" : inferResult.getClass()));
        }
        Map<?, ?> resultMap = (Map<?, ?>) inferResult;
        // node_id is required by protocol but we treat Java vertexId as authoritative.
        // If it exists and is numeric, do a best-effort sanity check.
        Object nodeId = resultMap.get("node_id");
        if (nodeId != null && vertexId instanceof Number && nodeId instanceof Number) {
            long expected = ((Number) vertexId).longValue();
            long actual = ((Number) nodeId).longValue();
            if (expected != actual) {
                throw new IllegalArgumentException(
                    "GCN infer result node_id mismatch, expected=" + vertexId + ", actual=" + nodeId);
            }
        }

        Integer prediction = asInteger(resultMap.get("prediction"));
        Double confidence = asDouble(resultMap.get("confidence"));
        Double[] embedding = asEmbedding(resultMap.get("embedding"));
        return ObjectRow.create(vertexId, embedding, prediction, confidence);
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid prediction value: " + value, e);
        }
    }

    private Double asDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid confidence or embedding value: " + value, e);
        }
    }

    private Double[] asEmbedding(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double[]) {
            return (Double[]) value;
        }
        List<Double> embeddingValues = new ArrayList<>();
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                embeddingValues.add(asDouble(item));
            }
        } else if (value instanceof Object[]) {
            for (Object item : (Object[]) value) {
                embeddingValues.add(asDouble(item));
            }
        } else {
            return null;
        }
        return embeddingValues.toArray(new Double[0]);
    }
}
