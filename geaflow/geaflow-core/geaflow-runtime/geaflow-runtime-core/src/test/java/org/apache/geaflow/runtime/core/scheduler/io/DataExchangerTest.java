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

package org.apache.geaflow.runtime.core.scheduler.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.cluster.response.ShardResult;
import org.apache.geaflow.core.graph.ExecutionEdge;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.message.ISliceMeta;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.Shard;
import org.testng.Assert;

public class DataExchangerTest extends TestCase {

    public void testBuildInput() {

        ExecutionVertex vertex = new ExecutionVertex(1, "test");
        int parallelism = 3;
        vertex.setParallelism(parallelism);
        int edgeId = 0;
        ExecutionEdge edge = new ExecutionEdge(null, edgeId, null, 0, 1, null);
        vertex.setInputEdges(Arrays.asList(edge));

        CycleResultManager resultManager = new CycleResultManager();
        ShardResult shards1 = new ShardResult(edgeId, OutputType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards1);
        ShardResult shards2 = new ShardResult(edgeId, OutputType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards2);

        Map<Integer, List<Shard>> result = DataExchanger.buildInput(vertex, edge, resultManager);
        Assert.assertEquals(parallelism, result.size());
        for (int i = 0; i < parallelism; i++) {
            Assert.assertEquals(result.get(i).size(), 1);
            Shard shard = result.get(i).get(0);
            Assert.assertEquals(shard.getSlices().size(), 2);
            for (ISliceMeta sliceMeta : shard.getSlices()) {
                Assert.assertEquals(i, sliceMeta.getSourceIndex());
                Assert.assertEquals(i, sliceMeta.getTargetIndex());
            }
        }
    }

    public void testBuildInputWithResponse() {

        ExecutionVertex vertex = new ExecutionVertex(1, "test");
        int parallelism = 3;
        vertex.setParallelism(parallelism);
        int edgeId = 0;
        ExecutionEdge edge = new ExecutionEdge(null, edgeId, null, 0, 1, null);

        CycleResultManager resultManager = new CycleResultManager();
        ShardResult shards1 = new ShardResult(edgeId, OutputType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards1);

        ShardResult shards2 = new ShardResult(edgeId, OutputType.FORWARD, buildSlices(parallelism));
        resultManager.register(0, shards2);

        ResponseResult response1 = new ResponseResult(edgeId, OutputType.RESPONSE, buildResponse());
        resultManager.register(1, response1);

        ResponseResult response2 = new ResponseResult(edgeId, OutputType.RESPONSE, buildResponse());
        resultManager.register(1, response2);

        // build input not include response.
        Map<Integer, List<Shard>> result = DataExchanger.buildInput(vertex, edge, resultManager);
        Assert.assertEquals(parallelism, result.size());
        for (int i = 0; i < parallelism; i++) {
            Assert.assertEquals(result.get(i).size(), 1);
            Shard shard = result.get(i).get(0);
            Assert.assertEquals(shard.getSlices().size(), 2);
            for (ISliceMeta sliceMeta : shard.getSlices()) {
                Assert.assertEquals(i, sliceMeta.getSourceIndex());
                Assert.assertEquals(i, sliceMeta.getTargetIndex());
            }
        }
    }


    private List<ISliceMeta> buildSlices(int parallelism) {
        List<ISliceMeta> slices = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            slices.add(new PipelineSliceMeta(i, i, 0, 0, null));
        }
        return slices;
    }

    private List<Object> buildResponse() {
        return Arrays.asList(0);
    }

}