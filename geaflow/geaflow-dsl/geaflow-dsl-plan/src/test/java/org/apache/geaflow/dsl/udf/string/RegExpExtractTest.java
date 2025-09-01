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

import org.apache.geaflow.dsl.udf.table.string.RegExpExtract;
import org.testng.annotations.Test;

public class RegExpExtractTest {

    @Test
    public void test() throws Exception {
        RegExpExtract udf = new RegExpExtract();
        udf.eval("252 - (_4Ped87iivN", ".*((.*))", 1);
        udf.eval("100-200", "(\\d+)-(\\d+)", 1);
        udf.eval("100-200", "(\\d+)-(\\d+)");
        udf.eval("100-200", "(\\d+)-(\\d+)", 1L);
        udf.eval("100-200", "(\\d+)-(\\d+)", "1");
        udf.eval("100-200", "", "1");
        udf.eval("100-200", "", 1);
        udf.eval("100-200", null);
        udf.eval("100-200", "(\\d+)-(\\d+)", 1L);
        udf.eval("100-200", "", 1L);
        udf.eval("100-200", null, 1L);
        udf.eval("100-200", "(\\d+)-(\\d+)", "-1");
        udf.eval("100-200", "(\\d+)-(\\d+)", -1);
    }
}
