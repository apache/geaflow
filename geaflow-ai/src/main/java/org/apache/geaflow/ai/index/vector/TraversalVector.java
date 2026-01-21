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

public class TraversalVector implements IVector {

    private final String[] vec;

    public TraversalVector(String... vec) {
        if (vec.length % 3 != 0) {
            throw new RuntimeException("Traversal vector should be src-edge-dst triple");
        }
        this.vec = vec;
    }

    @Override
    public double match(IVector other) {
        return 0;
    }

    @Override
    public VectorType getType() {
        return VectorType.TraversalVector;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TraversalVector{vec=");
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) {
                sb.append(i % 3 == 0 ? "; " : "-");
            }
            sb.append(vec[i]);
            if (i % 3 == 2) {
                sb.append(">");
            }
        }
        return sb.append('}').toString();
    }
}
