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

package org.apache.geaflow.dsl.udf.string;

import static org.testng.Assert.assertEquals;

import org.apache.geaflow.dsl.udf.table.string.GetJsonObject;
import org.testng.annotations.Test;

public class UDFGetJsonObjectTest {

    @Test
    public void test() {

        GetJsonObject udf = new GetJsonObject();

        String jsonStr = "{\"name\": \"Bob\", \"age\": 30, \"address\": " +
            "{\"city\": \"Los Angeles\", \"zip\": \"90001\"},\"items\": [\"item1\", \"item2\", \"item3\"]}";

        assertEquals("Bob", udf.eval(jsonStr, "name"));
        assertEquals("30", udf.eval(jsonStr, "$.age"));
        assertEquals("Los Angeles", udf.eval(jsonStr, "$.address.city"));
        assertEquals("item3", udf.eval(jsonStr, "$.items[2]"));
        assertEquals(null, udf.eval(jsonStr, "gender"));
        assertEquals(null, udf.eval(jsonStr, "$.items[3]"));
    }
}
