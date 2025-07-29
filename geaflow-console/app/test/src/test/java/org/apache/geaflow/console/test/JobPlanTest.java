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

import com.alibaba.fastjson.JSON;
import org.apache.geaflow.console.common.service.integration.engine.JsonPlan;
import org.apache.geaflow.console.core.model.release.JobPlan;
import org.apache.geaflow.console.core.model.release.JobPlanBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JobPlanTest {

    @Test
    public void test() {
        String plan = "{\"vertices\":{\"1\":{\"vertexType\":\"source\",\"id\":\"1\",\"parallelism\":1,\"parents\":[],"
            + "\"innerPlan\":{\"vertices\":{\"1-1\":{\"id\":\"1-1\",\"parallelism\":1,\"operator\":\"WindowStreamSourceOperator\","
            + "\"operatorName\":\"1\",\"parents\":[]},\"1-4\":{\"id\":\"1-4\",\"parallelism\":1,"
            + "\"operator\":\"KeySelectorStreamOperator\",\"operatorName\":\"4\",\"parents\":[{\"id\":\"1-1\"}]}}}},"
            + "\"2\":{\"vertexType\":\"source\",\"id\":\"2\",\"parallelism\":1,\"parents\":[],"
            + "\"innerPlan\":{\"vertices\":{\"2-2\":{\"id\":\"2-2\",\"parallelism\":1,\"operator\":\"WindowStreamSourceOperator\","
            + "\"operatorName\":\"2\",\"parents\":[]},\"2-5\":{\"id\":\"2-5\",\"parallelism\":1,"
            + "\"operator\":\"KeySelectorStreamOperator\",\"operatorName\":\"5\",\"parents\":[{\"id\":\"2-2\"}]}}}},"
            + "\"3\":{\"vertexType\":\"vertex_centric\",\"id\":\"3\",\"parallelism\":2,"
            + "\"operator\":\"StaticGraphVertexCentricTraversalAllOp\",\"operatorName\":\"GeaFlowStaticVCTraversal\","
            + "\"parents\":[{\"id\":\"2\",\"partitionType\":\"key\"},{\"id\":\"1\",\"partitionType\":\"key\"}]},"
            + "\"6\":{\"vertexType\":\"process\",\"id\":\"6\",\"parallelism\":2,\"parents\":[{\"id\":\"3\","
            + "\"partitionType\":\"forward\"}],\"innerPlan\":{\"vertices\":{\"6-7\":{\"id\":\"6-7\",\"parallelism\":2,"
            + "\"operator\":\"MapWindowOperator\",\"operatorName\":\"Project-1\",\"parents\":[{\"id\":\"6-6\"}]},\"6-8\":{\"id\":\"6-8\","
            + "\"parallelism\":2,\"operator\":\"SinkStreamOperator\",\"operatorName\":\"TableSink-2\",\"parents\":[{\"id\":\"6-7\"}]},"
            + "\"6-6\":{\"id\":\"6-6\",\"parallelism\":2,\"operator\":\"FlatMapStreamOperator\","
            + "\"operatorName\":\"TraversalResponseToRow-0\",\"parents\":[]}}}}}}";

        JsonPlan jsonPlan = JSON.parseObject(plan, JsonPlan.class);
        JobPlan jobPlan = JobPlanBuilder.build(jsonPlan);
        Assert.assertEquals(jobPlan.getVertices().size(), 4);
        Assert.assertEquals(jobPlan.getVertices().get("vertex_centric-3").getParallelism(), 2);
        Assert.assertEquals(jobPlan.getEdgeMap().get("source-1").get(0).getSourceKey(), "source-1");
    }

}
