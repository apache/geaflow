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
import java.util.Objects;

public class Vertex {

    private final String id;
    private final String label;
    private final List<String> values;

    public Vertex(String label, String id, List<String> values) {
        this.label = label;
        this.id = id;
        this.values = values;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return label + " | " + id + " | " + String.join("|", values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Vertex vertex = (Vertex) o;
        return Objects.equals(id, vertex.id) && Objects.equals(label, vertex.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label);
    }
}
