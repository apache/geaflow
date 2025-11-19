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

import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;

import java.util.ArrayList;
import java.util.List;

public class EntityAttributeIndexStore implements IndexStore {

    @Override
    public List<IVector> getVertexIndex(GraphVertex graphVertex) {
        Vertex vertex = graphVertex.getVertex();
        List<String> sentences  = vertex.getValues();
        sentences.add(vertex.getId());
        sentences.add(vertex.getLabel());
        List<String> frequencyWords = WordFrequencyProcessor.processSentences(sentences);
        KeywordVector keywordVector = new KeywordVector(frequencyWords.toArray(new String[0]));
        List<IVector> results = new ArrayList<>();
        results.add(keywordVector);
        return results;
    }

    @Override
    public List<IVector> getEdgeIndex(GraphEdge graphEdge) {
        Edge edge = graphEdge.getEdge();
        List<String> sentences  = edge.getValues();
        sentences.add(edge.getSrcId());
        sentences.add(edge.getDstId());
        sentences.add(edge.getLabel());
        List<String> frequencyWords = WordFrequencyProcessor.processSentences(sentences);
        KeywordVector keywordVector = new KeywordVector(frequencyWords.toArray(new String[0]));
        List<IVector> results = new ArrayList<>();
        results.add(keywordVector);
        return results;
    }
}
