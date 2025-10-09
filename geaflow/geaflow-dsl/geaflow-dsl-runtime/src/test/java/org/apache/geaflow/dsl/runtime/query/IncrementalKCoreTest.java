/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License.  You may obtain a copy of the License at
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

package org.apache.geaflow.dsl.runtime.query;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.Test;

/**
 * Incremental K-Core algorithm test class
 * Includes basic functionality tests, incremental update tests, dynamic graph tests, etc.
 * 
 * @author TuGraph Analytics Team
 */
public class IncrementalKCoreTest {

    @Test
    public void testIncrementalKCore_001_Basic() throws Exception {
        // Note: Currently only this test can run stably
        // Other tests are disabled due to GeaFlow framework RPC communication issues
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrementalKCore_002_IncrementalUpdate() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrementalKCore_003_EdgeAddition() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/dynamic_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrementalKCore_004_Performance() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/large_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrementalKCore_005_ComplexTopology() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/complex_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrementalKCore_006_DisconnectedComponents() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/disconnected_graph.sql")
            .withQueryPath("/query/gql_inc_kcore_010.sql")
            .execute()
            .checkSinkResult();
    }
}
