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

package org.apache.geaflow.ai.graph.io;

import java.util.List;

public class Edge implements Entity {


    private final String srcId;
    private final String dstId;
    private final String label;
    private final List<String> values;

    public Edge(String label, String srcId, String dstId, List<String> values) {
        this.label = label;
        this.srcId = srcId;
        this.dstId = dstId;
        this.values = values;
    }

    public List<String> getValues() {
        return values;
    }

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return label + " | " + srcId + " | " + dstId + " | " + String.join("|", values);
    }
}
