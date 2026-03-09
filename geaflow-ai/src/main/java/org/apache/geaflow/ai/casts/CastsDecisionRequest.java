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

package org.apache.geaflow.ai.casts;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import java.util.Map;

public class CastsDecisionRequest {

    public String goal;

    @SerializedName("max_depth")
    public Integer maxDepth;

    public Traversal traversal;

    public Node node;

    @SerializedName("graph_schema")
    public GraphSchema graphSchema;

    public static class Traversal {
        @SerializedName("structural_signature")
        public String structuralSignature;

        @SerializedName("step_index")
        public Integer stepIndex;
    }

    public static class Node {
        public String label;
        public Map<String, Object> properties;
    }

    public static class GraphSchema {
        @SerializedName("schema_fingerprint")
        public String schemaFingerprint;

        @SerializedName("valid_outgoing_labels")
        public List<String> validOutgoingLabels;

        @SerializedName("valid_incoming_labels")
        public List<String> validIncomingLabels;

        @SerializedName("schema_summary")
        public String schemaSummary;
    }
}

