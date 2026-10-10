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

package org.apache.geaflow.ai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class GeaFlowMemoryServerTest {

    @Test
    void preservesValidRequestIdAndRegeneratesInvalidValues() {
        assertEquals("trace-1", GeaFlowMemoryServer.resolveRequestId(" trace-1 "));
        assertNotEquals("", GeaFlowMemoryServer.resolveRequestId("  "));
        assertNotEquals("", GeaFlowMemoryServer.resolveRequestId(null));
        assertNotEquals("", GeaFlowMemoryServer.resolveRequestId(repeat('x', 129)));
        assertNotEquals("trace/1", GeaFlowMemoryServer.resolveRequestId("trace/1"));
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int index = 0; index < count; index++) {
            result.append(value);
        }
        return result.toString();
    }
}
