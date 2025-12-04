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

package org.apache.geaflow.dsl.connector.odps;

import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPartitionExtractorTest {

    @Test
    public void testExtract() {
        Row row1 = ObjectRow.create(1, 2025111, 10, 3.14, "a11");
        Row row2 = ObjectRow.create(1, 2025111, 11, 3.14, "b11");
        Row row3 = ObjectRow.create(1, 2025111, 12, 3.14, "c11");
        Row row4 = ObjectRow.create(1, 2025111, 13, 3.14, "d11");
        Row row5 = ObjectRow.create(1, 2025111, 14, 3.14, "e11");
        Row row6 = ObjectRow.create(1, 2025110, 11, 3.14, "null");

        String spec1 = "dt=$dt,hh=$hh,biz=$biz";
        DefaultPartitionExtractor extractor1 = new DefaultPartitionExtractor(
                spec1, new int[]{1, 2, 4},
                new IType[]{IntegerType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE});
        Assert.assertEquals("dt=2025111,hh=10,biz=a11", extractor1.extractPartition(row1));
        Assert.assertEquals("dt=2025111,hh=11,biz=b11", extractor1.extractPartition(row2));
        Assert.assertEquals("dt=2025111,hh=12,biz=c11", extractor1.extractPartition(row3));
        Assert.assertEquals("dt=2025111,hh=13,biz=d11", extractor1.extractPartition(row4));
        Assert.assertEquals("dt=2025111,hh=14,biz=e11", extractor1.extractPartition(row5));
        Assert.assertEquals("dt=2025110,hh=11,biz=null", extractor1.extractPartition(row6));

        String spec2 = "dt=$dt/hh=$hh/biz=$biz";
        DefaultPartitionExtractor extractor2 = new DefaultPartitionExtractor(
                spec2, new int[]{1, 2, 4},
                new IType[]{IntegerType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE});
        Assert.assertEquals("dt=2025111/hh=10/biz=a11", extractor2.extractPartition(row1));
        Assert.assertEquals("dt=2025111/hh=11/biz=b11", extractor2.extractPartition(row2));
        Assert.assertEquals("dt=2025111/hh=12/biz=c11", extractor2.extractPartition(row3));
        Assert.assertEquals("dt=2025111/hh=13/biz=d11", extractor2.extractPartition(row4));
        Assert.assertEquals("dt=2025111/hh=14/biz=e11", extractor2.extractPartition(row5));
        Assert.assertEquals("dt=2025110/hh=11/biz=null", extractor2.extractPartition(row6));


        String spec3 = "dt=$dt";
        DefaultPartitionExtractor extractor3 = new DefaultPartitionExtractor(
                spec3, new int[]{1},
                new IType[]{IntegerType.INSTANCE});
        Assert.assertEquals("dt=2025111", extractor3.extractPartition(row1));
        Assert.assertEquals("dt=2025111", extractor3.extractPartition(row2));
        Assert.assertEquals("dt=2025111", extractor3.extractPartition(row3));
        Assert.assertEquals("dt=2025111", extractor3.extractPartition(row4));
        Assert.assertEquals("dt=2025111", extractor3.extractPartition(row5));
        Assert.assertEquals("dt=2025110", extractor3.extractPartition(row6));


        String spec4 = "dt=20251120";
        DefaultPartitionExtractor extractor4 = new DefaultPartitionExtractor(
                spec4, new int[]{},
                new IType[]{});
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row1));
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row2));
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row3));
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row4));
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row5));
        Assert.assertEquals("dt=20251120", extractor4.extractPartition(row6));

        String spec5 = "dt=20251120,hh=$hh";
        DefaultPartitionExtractor extractor5 = new DefaultPartitionExtractor(
                spec5, new int[]{2},
                new IType[]{IntegerType.INSTANCE});
        Assert.assertEquals("dt=20251120,hh=10", extractor5.extractPartition(row1));
        Assert.assertEquals("dt=20251120,hh=11", extractor5.extractPartition(row2));
        Assert.assertEquals("dt=20251120,hh=12", extractor5.extractPartition(row3));
        Assert.assertEquals("dt=20251120,hh=13", extractor5.extractPartition(row4));
        Assert.assertEquals("dt=20251120,hh=14", extractor5.extractPartition(row5));
        Assert.assertEquals("dt=20251120,hh=11", extractor5.extractPartition(row6));

        PartitionExtractor extractor6 = DefaultPartitionExtractor.create("", null);
        Assert.assertEquals("", extractor6.extractPartition(row1));
        Assert.assertEquals("", extractor6.extractPartition(row2));
        Assert.assertEquals("", extractor6.extractPartition(row3));
        Assert.assertEquals("", extractor6.extractPartition(row4));
        Assert.assertEquals("", extractor6.extractPartition(row5));
        Assert.assertEquals("", extractor6.extractPartition(row6));

    }

    @Test
    public void testUnquoted() {
        Assert.assertEquals("dt", DefaultPartitionExtractor.unquoted("dt"));
        Assert.assertEquals("dt", DefaultPartitionExtractor.unquoted("`dt`"));
        Assert.assertEquals("dt", DefaultPartitionExtractor.unquoted("'dt'"));
        Assert.assertEquals("dt", DefaultPartitionExtractor.unquoted("\"dt\""));
    }
}
