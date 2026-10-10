/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.index.vectorstore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VectorQuery {
    private final double[] queryVector;
    private final int topK;
    private final Map<String, String> filterMetadata;

    public VectorQuery(double[] queryVector, int topK, Map<String, String> filterMetadata) {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("queryVector cannot be null or empty");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be greater than 0");
        }
        this.queryVector = queryVector.clone();
        this.topK = topK;
        this.filterMetadata = filterMetadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(filterMetadata));
    }

    public double[] getQueryVector() {
        return queryVector.clone();
    }

    public int getTopK() {
        return topK;
    }

    public Map<String, String> getFilterMetadata() {
        return filterMetadata;
    }
}
