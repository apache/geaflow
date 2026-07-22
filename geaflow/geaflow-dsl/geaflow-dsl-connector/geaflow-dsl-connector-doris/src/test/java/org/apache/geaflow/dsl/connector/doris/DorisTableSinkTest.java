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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DorisTableSinkTest {

    private DorisTableSink sink;
    private StructType schema;

    @BeforeMethod
    public void setUp() {
        sink = new DorisTableSink();
        schema = new StructType(
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, true));
    }

    private Configuration validConfig() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_FENODES, "127.0.0.1:8030");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, "test_db");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE, "test_table");
        return conf;
    }

    @Test
    public void testInit() {
        sink.init(validConfig(), schema);
        Assert.assertNotNull(sink);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class)
    public void testInitWithoutFeNodes() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, "test_db");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE, "test_table");
        sink.init(conf, schema);
    }
}
