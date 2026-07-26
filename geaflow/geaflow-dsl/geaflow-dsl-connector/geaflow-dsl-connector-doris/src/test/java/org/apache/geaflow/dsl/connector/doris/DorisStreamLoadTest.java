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

package org.apache.geaflow.dsl.connector.doris;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DorisStreamLoadTest {

    private DorisStreamLoad newClient(java.util.List<String> feNodes) {
        return new DorisStreamLoad(feNodes, "test_db", "test_table", "root", "",
            DorisConstants.FORMAT_CSV, "\t", "\n", Arrays.asList("id", "name"), 1000, 1000, 3);
    }

    @Test
    public void testLoadUrlWithHostPort() throws IOException {
        try (DorisStreamLoad client = newClient(Collections.singletonList("127.0.0.1:8030"))) {
            Assert.assertEquals(client.getLoadUrl(),
                "http://127.0.0.1:8030/api/test_db/test_table/_stream_load");
        }
    }

    @Test
    public void testLoadUrlWithScheme() throws IOException {
        try (DorisStreamLoad client = newClient(Collections.singletonList("http://doris-fe:8030"))) {
            Assert.assertEquals(client.getLoadUrl(),
                "http://doris-fe:8030/api/test_db/test_table/_stream_load");
        }
    }

    @Test
    public void testMultipleFeNodesForFailover() throws IOException {
        try (DorisStreamLoad client =
                 newClient(Arrays.asList("fe1:8030", "fe2:8030", "fe3:8030"))) {
            Assert.assertEquals(client.getLoadUrls().size(), 3);
            Assert.assertEquals(client.getLoadUrls().get(1),
                "http://fe2:8030/api/test_db/test_table/_stream_load");
        }
    }

    @Test(expectedExceptions = GeaFlowDSLException.class)
    public void testEmptyFeNodesThrows() throws IOException {
        try (DorisStreamLoad client = newClient(Collections.emptyList())) {
            Assert.fail("should have thrown for empty fenodes");
        }
    }
}
