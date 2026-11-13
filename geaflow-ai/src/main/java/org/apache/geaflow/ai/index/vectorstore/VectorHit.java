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

public class VectorHit {
    private final String vectorId;
    private final double score;
    private final VectorRecord record;

    public VectorHit(String vectorId, double score, VectorRecord record) {
        this.vectorId = vectorId;
        this.score = score;
        this.record = record;
    }

    public String getVectorId() {
        return vectorId;
    }

    public double getScore() {
        return score;
    }

    public VectorRecord getRecord() {
        return record;
    }
}
