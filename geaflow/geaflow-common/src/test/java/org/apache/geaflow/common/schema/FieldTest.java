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

package org.apache.geaflow.common.schema;

import org.apache.geaflow.common.type.primitive.BooleanType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldTest {

    private static final String FIELD_NAME = "f";

    @Test
    public void testField() {

        Field.BooleanField field0 = Field.newBooleanField(FIELD_NAME);
        Assert.assertEquals(field0.getName(), FIELD_NAME);
        Assert.assertEquals(field0.getType(), BooleanType.INSTANCE);
        Assert.assertTrue(field0.isNullable());
        Assert.assertNull(field0.getDefaultValue());

        Field.BooleanField field1 = Field.newBooleanField(FIELD_NAME, Boolean.TRUE, Boolean.FALSE);
        Field.ByteField field2 = Field.newByteField(FIELD_NAME);
        Field.ByteField field3 = Field.newByteField(FIELD_NAME, Boolean.TRUE, (byte) 0);
        Field.ShortField field4 = Field.newShortField(FIELD_NAME);
        Field.ShortField field5 = Field.newShortField(FIELD_NAME, Boolean.TRUE, (short) 0);
        Field.IntegerField field6 = Field.newIntegerField(FIELD_NAME);
        Field.IntegerField field7 = Field.newIntegerField(FIELD_NAME, Boolean.TRUE, 0);
        Field.LongField field8 = Field.newLongField(FIELD_NAME);
        Field.LongField field9 = Field.newLongField(FIELD_NAME, Boolean.TRUE, 0L);
        Field.FloatField field10 = Field.newFloatField(FIELD_NAME);
        Field.FloatField field11 = Field.newFloatField(FIELD_NAME, Boolean.TRUE, 0.0f);
        Field.DoubleField field12 = Field.newDoubleField(FIELD_NAME);
        Field.DoubleField field13 = Field.newDoubleField(FIELD_NAME, Boolean.TRUE, 0.0);
        Field.StringField field14 = Field.newStringField(FIELD_NAME);
        Field.StringField field15 = Field.newStringField(FIELD_NAME, Boolean.TRUE, "");
        Assert.assertNotNull(field1);
        Assert.assertNotNull(field2);
        Assert.assertNotNull(field3);
        Assert.assertNotNull(field4);
        Assert.assertNotNull(field5);
        Assert.assertNotNull(field6);
        Assert.assertNotNull(field7);
        Assert.assertNotNull(field8);
        Assert.assertNotNull(field9);
        Assert.assertNotNull(field10);
        Assert.assertNotNull(field11);
        Assert.assertNotNull(field12);
        Assert.assertNotNull(field13);
        Assert.assertNotNull(field14);
        Assert.assertNotNull(field15);
    }

}
