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

import java.util.Arrays;
import java.util.Objects;

public class EmbeddingVector implements IVector {


    private final double[] vec;

    public EmbeddingVector(double[] vec) {
        this.vec = Objects.requireNonNull(vec);
    }

    @Override
    public double match(IVector other) {
        // Type check: must be same implementation class
        if (!(other instanceof EmbeddingVector)) {
            return 0.0;
        }
        EmbeddingVector otherVec = (EmbeddingVector) other;

        // Dimension check: vectors must have same length
        if (this.vec.length != otherVec.vec.length) {
            return 0.0;
        }

        double dotProduct = 0.0;  // Accumulator for dot product
        double normSquared1 = 0.0; // Accumulator for squared L2 norm of this vector
        double normSquared2 = 0.0; // Accumulator for squared L2 norm of other vector

        // Single-pass computation for dot product and squared norms
        for (int i = 0; i < this.vec.length; i++) {
            dotProduct += this.vec[i] * otherVec.vec[i];
            normSquared1 += this.vec[i] * this.vec[i];
            normSquared2 += otherVec.vec[i] * otherVec.vec[i];
        }

        // Calculate denominator (product of L2 norms)
        double denominator = Math.sqrt(normSquared1) * Math.sqrt(normSquared2);

        // Handle zero-vector case (avoid division by zero)
        if (denominator == 0.0) {
            return 0.0;
        }

        // Return cosine similarity: dot product divided by norms product
        return dotProduct / denominator;
    }

    @Override
    public VectorType getType() {
        return VectorType.EmbeddingVector;
    }

    public double[] getVec() {
        return vec;
    }

    @Override
    public String toString() {
        return "EmbeddingVector{"
                + "vec=" + Arrays.toString(vec)
                + '}';
    }
}
