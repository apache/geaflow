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

package org.apache.geaflow.ai.protocol;

import com.google.gson.annotations.SerializedName;
import java.util.UUID;

public class AiTrace {

    @SerializedName("trace_id")
    public String traceId;

    @SerializedName("timestamp")
    public Double timestamp;

    @SerializedName("caller")
    public String caller;

    public static AiTrace newTrace(String caller) {
        AiTrace trace = new AiTrace();
        trace.traceId = "tr_" + UUID.randomUUID().toString().replace("-", "");
        trace.timestamp = System.currentTimeMillis() / 1000.0;
        trace.caller = caller;
        return trace;
    }
}

