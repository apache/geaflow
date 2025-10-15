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

package org.apache.geaflow.dsl.connector.api;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.serde.impl.JsonDeserializer;
import org.junit.Test;
import org.testng.Assert;

public class JsonDeserializerTest {

    @Test
    public void testDeserialize() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> row = deserializer.deserialize("{\"id\":1, \"name\":\"amy\", \"age\":10}");
        List<Row> rowWithNull = deserializer.deserialize("{\"id\":1, \"name\":\"amy\"}");
        Assert.assertEquals(row.get(0).getField(0, IntegerType.INSTANCE), 1);
        Assert.assertEquals(row.get(0).getField(1, BinaryStringType.INSTANCE).toString(), "amy");
        Assert.assertEquals(row.get(0).getField(2, IntegerType.INSTANCE), 10);
        Assert.assertEquals(rowWithNull.get(0).getField(0, IntegerType.INSTANCE), 1);
        Assert.assertEquals(rowWithNull.get(0).getField(1, BinaryStringType.INSTANCE).toString(), "amy");
        Assert.assertEquals(rowWithNull.get(0).getField(2, IntegerType.INSTANCE), null);

    }


    @Test
    public void testDeserializeEmptyString() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> rows = deserializer.deserialize("");
        List<Row> testNullRows = deserializer.deserialize(null);
        Assert.assertEquals(rows, Collections.emptyList());
        Assert.assertEquals(testNullRows, Collections.emptyList());

    }

    @Test(expected = GeaflowRuntimeException.class)
    public void testDeserializeParseError() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> rows = deserializer.deserialize("test");
    }

    @Test
    public void testDeserializeIgnoreParseError() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        Configuration conf = new Configuration();
        conf.put("geaflow.dsl.connector.format.json.ignore-parse-error", "true");
        deserializer.init(conf, dataSchema);
        List<Row> rows = deserializer.deserialize("test");
        Assert.assertEquals(rows, Collections.emptyList());
    }

    @Test(expected = GeaflowRuntimeException.class)
    public void testDeserializeFailOnMissingField() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        Configuration conf = new Configuration();
        conf.put("geaflow.dsl.connector.format.json.fail-on-missing-field", "true");
        deserializer.init(conf, dataSchema);
        List<Row> rowWithMissingField = deserializer.deserialize("{\"id\":1, \"name\":\"amy\"}");

    }


}
