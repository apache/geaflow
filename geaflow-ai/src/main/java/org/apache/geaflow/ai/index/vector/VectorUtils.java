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

package org.apache.geaflow.ai.index.vector;

import java.util.ArrayList;
import java.util.List;

public class VectorUtils {

    /**
     * Compute normalized average vector (A+B)/2
     * Represents the common content between two vectors.
     */
    public static List<IVector> common(List<IVector> vectorListA, List<IVector> vectorListB) {
        List<IVector> result = new ArrayList<>();
        for (int i = 0; i < vectorListA.size(); i++) {
            if (!(vectorListA.get(i) instanceof EmbeddingVector)) {
                continue;
            }
            EmbeddingVector vecA = (EmbeddingVector) vectorListA.get(i);
            for (int l = 0; l < vectorListB.size(); l++) {
                if (!(vectorListB.get(l) instanceof EmbeddingVector)) {
                    continue;
                }
                EmbeddingVector vecB = (EmbeddingVector) vectorListB.get(l);

                double[] avgVec = new double[vecA.getVec().length];
                for (int j = 0; j < vecA.getVec().length; j++) {
                    avgVec[j] = (vecA.getVec()[j] + vecB.getVec()[j]) / 2;
                }

                // Normalize the average vector
                result.add(normalize(new EmbeddingVector(avgVec)));
            }
        }

        return result;
    }

    /**
     * Compute normalized difference vector A-B
     * Represents the change direction from B to A.
     */
    public static List<IVector> diff(List<IVector> vectorListA, List<IVector> vectorListB) {
        List<IVector> result = new ArrayList<>();
        for (int i = 0; i < vectorListA.size(); i++) {
            if (!(vectorListA.get(i) instanceof EmbeddingVector)) {
                continue;
            }
            EmbeddingVector vecA = (EmbeddingVector) vectorListA.get(i);
            for (int l = 0; l < vectorListB.size(); l++) {
                if (!(vectorListB.get(l) instanceof EmbeddingVector)) {
                    continue;
                }
                EmbeddingVector vecB = (EmbeddingVector) vectorListB.get(l);

                double[] diffVec = new double[vecA.getVec().length];
                for (int j = 0; j < vecA.getVec().length; j++) {
                    diffVec[j] = vecA.getVec()[j] - vecB.getVec()[j];
                }

                // Normalize the difference vector
                result.add(normalize(new EmbeddingVector(diffVec)));
            }
        }

        return result;
    }

    /**
     * Compute normalized orthogonal difference vector A - (A·B)B
     * Represents the unique content in A that is independent of B.
     */
    public static List<IVector> unique(List<IVector> vectorListA, List<IVector> vectorListB) {
        List<IVector> result = new ArrayList<>();
        for (int i = 0; i < vectorListA.size(); i++) {
            if (!(vectorListA.get(i) instanceof EmbeddingVector)) {
                continue;
            }
            EmbeddingVector vecA = (EmbeddingVector) vectorListA.get(i);
            for (int l = 0; l < vectorListB.size(); l++) {
                if (!(vectorListB.get(l) instanceof EmbeddingVector)) {
                    continue;
                }
                EmbeddingVector vecB = (EmbeddingVector) vectorListB.get(l);
                // Compute dot product A·B
                double dotProduct = 0.0;
                for (int j = 0; j < vecA.getVec().length; j++) {
                    dotProduct += vecA.getVec()[j] * vecB.getVec()[j];
                }

                // Compute projection (A·B)B
                double[] projection = new double[vecA.getVec().length];
                for (int j = 0; j < vecA.getVec().length; j++) {
                    projection[j] = dotProduct * vecB.getVec()[j];
                }

                // Compute orthogonal difference A - (A·B)B
                double[] uniqueVec = new double[vecA.getVec().length];
                for (int j = 0; j < vecA.getVec().length; j++) {
                    uniqueVec[j] = vecA.getVec()[j] - projection[j];
                }

                // Normalize the orthogonal difference vector
                result.add(normalize(new EmbeddingVector(uniqueVec)));
            }
        }

        return result;
    }

    /**
     * Normalize vector (convert to unit vector).
     */
    private static EmbeddingVector normalize(EmbeddingVector vector) {
        double[] vec = vector.getVec();

        // Compute L2 norm
        double norm = 0.0;
        for (double v : vec) {
            norm += v * v;
        }
        norm = Math.sqrt(norm);

        // Return original vector if norm is zero (avoid division by zero)
        if (norm == 0.0) {
            return vector;
        }

        // Normalize the vector
        double[] normalized = new double[vec.length];
        for (int i = 0; i < vec.length; i++) {
            normalized[i] = vec[i] / norm;
        }

        return new EmbeddingVector(normalized);
    }
}
