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

import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DorisUtilsTest {

    private StructType buildSchema() {
        return new StructType(
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, true),
            new TableField("score", Types.DOUBLE, true));
    }

    @Test
    public void testRowToCsv() {
        StructType schema = buildSchema();
        Row row = ObjectRow.create(1L, "alice", 9.5);
        String csv = DorisUtils.rowToCsv(row, schema, "\t");
        Assert.assertEquals(csv, "1\talice\t9.5");
    }

    @Test
    public void testRowToCsvWithNull() {
        StructType schema = buildSchema();
        Row row = ObjectRow.create(2L, null, null);
        String csv = DorisUtils.rowToCsv(row, schema, "\t");
        Assert.assertEquals(csv, "2\t\\N\t\\N");
    }

    @Test
    public void testRowToCsvWithCustomSeparator() {
        StructType schema = buildSchema();
        Row row = ObjectRow.create(3L, "bob", 1.0);
        String csv = DorisUtils.rowToCsv(row, schema, ",");
        Assert.assertEquals(csv, "3,bob,1.0");
    }

    @Test
    public void testRowToJson() {
        StructType schema = buildSchema();
        Row row = ObjectRow.create(4L, "carol", 8.0);
        String json = DorisUtils.rowToJson(row, schema);
        Assert.assertTrue(json.contains("\"id\":4"));
        Assert.assertTrue(json.contains("\"name\":\"carol\""));
        Assert.assertTrue(json.contains("\"score\":8.0"));
    }
}
