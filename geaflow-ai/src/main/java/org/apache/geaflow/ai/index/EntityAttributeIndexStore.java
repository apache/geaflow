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

package org.apache.geaflow.ai.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

public class EntityAttributeIndexStore implements IndexStore {

    private VerbalizationFunction verbFunc;

    public void initStore(VerbalizationFunction func) {
        if (func != null) {
            this.verbFunc = func;
        }
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            String verbalization = verbFunc.verbalize(new SubGraph().addVertex((GraphVertex) entity));
            List<String> sentences = new ArrayList<>();
            sentences.add(verbalization);
            KeywordVector keywordVector = new KeywordVector(sentences.toArray(new String[0]));
            List<IVector> results = new ArrayList<>();
            results.add(keywordVector);
            return results;
        } else {
            String verbalization = verbFunc.verbalize(new SubGraph().addEdge((GraphEdge) entity));
            List<String> sentences = new ArrayList<>();
            sentences.add(verbalization);
            KeywordVector keywordVector = new KeywordVector(sentences.toArray(new String[0]));
            List<IVector> results = new ArrayList<>();
            results.add(keywordVector);
            return results;
        }
    }
}
