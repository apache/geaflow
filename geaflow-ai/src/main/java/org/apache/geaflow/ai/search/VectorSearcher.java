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

package org.apache.geaflow.ai.search;

import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VectorSearcher {

    private final double threshold = 1.0;

    private final VectorSearch search;
    private final List<GraphEntity> entityList = new ArrayList<>();
    private final List<List<IVector>> entityIndex = new ArrayList<>();

    public VectorSearcher(VectorSearch search) {
        this.search = search;
    }

    public void add(GraphEntity entity, List<IVector> entityVectors) {
        this.entityList.add(entity);
        this.entityIndex.add(entityVectors);
    }

    public List<GraphEntity> getResults() {
        List<GraphEntity> entityList = new ArrayList<>();
        for (int i = 0; i < this.entityList.size(); i++) {
            List<IVector> index = this.entityIndex.get(i);
            double entityMark = 0.0;
            for (Map.Entry<VectorType, List<IVector>> entry : search.getVectorList().entrySet()) {
                VectorType searchType = entry.getKey();
                for (IVector indexVector : index) {
                    if (indexVector.getType() == searchType) {
                        List<IVector> searchVectors  = entry.getValue();
                        for (IVector searchVector : searchVectors) {
                            entityMark += searchVector.match(indexVector);
                        }
                    }
                }
            }
            if (entityMark > threshold) {
                GraphEntity entity = this.entityList.get(i);
                entityList.add(entity);
            }
        }
        return entityList;
    }
}
