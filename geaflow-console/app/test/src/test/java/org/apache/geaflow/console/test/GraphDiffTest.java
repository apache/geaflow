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

package org.apache.geaflow.console.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphDiffTest {

    @Test
    public void testDiff() {
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        list1.add("1");
        list1.add("2");
        list1.add("3");

        list2.add("3");
        list2.add("4");
        List<String> diff1 = ListUtil.diff(list1, list2);
        List<String> diff2 = ListUtil.diff(list2, list1);
        Assert.assertEquals(diff1.size(), 2);
        Assert.assertEquals(diff2.size(), 1);
    }

    @Test
    public void testDiff2() {
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        list1.add("1");
        list1.add("2");
        list1.add("2");
        list1.add("2");
        list1.add("3");

        list2.add("3");
        list2.add("3");
        list2.add("4");
        List<String> diff1 = ListUtil.diff(list1, list2);
        List<String> diff2 = ListUtil.diff(list2, list1);
        Assert.assertEquals(diff1.size(), 4);
        Assert.assertEquals(diff2.size(), 2);
    }

    @Test
    public void testNameDiff() {
        GeaflowGraph g1 = new GeaflowGraph("g1", null);
        g1.setInstanceId("1");
        GeaflowGraph g2 = new GeaflowGraph("g1", null);
        g2.setInstanceId("2");
        GeaflowGraph g3 = new GeaflowGraph("g1", null);
        g3.setInstanceId("3");
        GeaflowGraph g4 = new GeaflowGraph("g1", null);
        g4.setInstanceId("1");
        List<GeaflowGraph> list1 = Arrays.asList(g1, g2);
        List<GeaflowGraph> list2 = Arrays.asList(g2, g3, g4);

        List<GeaflowGraph> diff1 = ListUtil.diff(list1, list2);
        List<GeaflowGraph> diff2 = ListUtil.diff(list2, list1);
        Assert.assertEquals(diff1.size(), 0);
        Assert.assertEquals(diff2.size(), 1);

        List<GeaflowGraph> diff3 = ListUtil.diff(list1, list2, graph -> graph.getName() + "-" + graph.getInstanceId());
        List<GeaflowGraph> diff4 = ListUtil.diff(list2, list1, graph -> graph.getName() + "-" + graph.getInstanceId());

        Assert.assertEquals(diff3.size(), 0);
        Assert.assertEquals(diff4.get(0).getInstanceId(), "3");
    }

    @Test
    public void testStructMappingDiff() {
        List<StructMapping> structMappings = new ArrayList<>();
        structMappings.add(new StructMapping("t1", "v1", null));
        structMappings.add(new StructMapping("t1", "v1", null));
        structMappings.add(new StructMapping("t2", "v2", null));
        structMappings.add(new StructMapping("t2", "v2", null));

        List<StructMapping> distinctList = structMappings.stream().distinct().collect(Collectors.toList());
        List<StructMapping> diff = ListUtil.diff(structMappings, distinctList);
        Assert.assertEquals(diff.size(), 2);

    }

}
