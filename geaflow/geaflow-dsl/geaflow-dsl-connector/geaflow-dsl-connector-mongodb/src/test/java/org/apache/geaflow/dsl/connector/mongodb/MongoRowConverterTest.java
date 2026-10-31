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

package org.apache.geaflow.dsl.connector.mongodb;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MongoRowConverterTest {

    private static final StructType SCHEMA = new StructType(
        new TableField("_id", Types.BINARY_STRING),
        new TableField("name", Types.BINARY_STRING),
        new TableField("count", Types.INTEGER),
        new TableField("total", Types.LONG),
        new TableField("score", Types.DOUBLE),
        new TableField("active", Types.BOOLEAN),
        new TableField("price", Types.DECIMAL),
        new TableField("created", Types.TIMESTAMP),
        new TableField("day", Types.DATE));

    @Test
    public void testDocumentToRow() {
        ObjectId id = new ObjectId();
        java.util.Date time = new java.util.Date(1710000000000L);
        Document document = new Document("_id", id)
            .append("name", "alice")
            .append("count", 3)
            .append("total", 8L)
            .append("score", 1.5D)
            .append("active", true)
            .append("price", new Decimal128(new BigDecimal("12.30")))
            .append("created", time)
            .append("day", time);

        Row row = new MongoRowConverter(SCHEMA).toRow(document);

        Assert.assertEquals(row.getField(0, Types.BINARY_STRING),
            BinaryString.fromString(id.toHexString()));
        Assert.assertEquals(row.getField(1, Types.BINARY_STRING),
            BinaryString.fromString("alice"));
        Assert.assertEquals(row.getField(2, Types.INTEGER), 3);
        Assert.assertEquals(row.getField(3, Types.LONG), 8L);
        Assert.assertEquals(row.getField(4, Types.DOUBLE), 1.5D);
        Assert.assertEquals(row.getField(5, Types.BOOLEAN), true);
        Assert.assertEquals(row.getField(6, Types.DECIMAL), new BigDecimal("12.30"));
        Assert.assertEquals(((Timestamp) row.getField(7, Types.TIMESTAMP)).getTime(), time.getTime());
        Assert.assertEquals(((Date) row.getField(8, Types.DATE)).getTime(), time.getTime());
    }

    @Test
    public void testRowToDocument() {
        Timestamp timestamp = new Timestamp(1710000000000L);
        Date date = new Date(1710028800000L);
        Row row = ObjectRow.create(
            BinaryString.fromString("record-1"),
            BinaryString.fromString("alice"),
            3,
            8L,
            1.5D,
            true,
            new BigDecimal("12.30"),
            timestamp,
            date);

        Document document = new MongoRowConverter(SCHEMA).toDocument(row);

        Assert.assertEquals(document.getString("_id"), "record-1");
        Assert.assertEquals(document.getString("name"), "alice");
        Assert.assertEquals(document.getInteger("count"), Integer.valueOf(3));
        Assert.assertEquals(document.getLong("total"), Long.valueOf(8));
        Assert.assertEquals(document.getDouble("score"), Double.valueOf(1.5D));
        Assert.assertEquals(document.getBoolean("active"), Boolean.TRUE);
        Assert.assertEquals(document.get("price"), new Decimal128(new BigDecimal("12.30")));
        Assert.assertEquals(document.getDate("created").getTime(), timestamp.getTime());
        Assert.assertEquals(document.getDate("day").getTime(), date.getTime());
    }

    @Test
    public void testExactDecimalToLong() {
        StructType schema = new StructType(new TableField("value", Types.LONG));
        Document document = new Document("value",
            new Decimal128(new BigDecimal("9007199254740992")));

        Row row = new MongoRowConverter(schema).toRow(document);

        Assert.assertEquals(row.getField(0, Types.LONG), 9007199254740992L);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*value.*Double.*FLOAT.*")
    public void testRejectFloatOverflow() {
        StructType schema = new StructType(new TableField("value", Types.FLOAT));
        Document document = new Document("value", Double.MAX_VALUE);

        new MongoRowConverter(schema).toRow(document);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*value.*Decimal128.*LONG.*")
    public void testRejectFractionalDecimalAsLong() {
        StructType schema = new StructType(new TableField("value", Types.LONG));
        Document document = new Document("value",
            new Decimal128(new BigDecimal("9007199254740992.1")));

        new MongoRowConverter(schema).toRow(document);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*name.*Document.*BINARY_STRING.*")
    public void testRejectNestedDocument() {
        StructType schema = new StructType(new TableField("name", Types.BINARY_STRING));
        Document document = new Document("name", new Document("first", "alice"));

        new MongoRowConverter(schema).toRow(document);
    }
}
