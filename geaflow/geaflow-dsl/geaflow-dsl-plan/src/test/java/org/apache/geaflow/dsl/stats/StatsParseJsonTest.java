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

package org.apache.geaflow.dsl.stats;

import com.google.gson.Gson;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.descriptor.NodeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.RelationDescriptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StatsParseJsonTest {

    @Test
    public void testParseUserStats() {
        GraphDescriptor userStats = new GraphDescriptor();
        userStats.addNode(new NodeDescriptor("n1", "Person"));
        userStats.addEdge(new EdgeDescriptor("e1", "knows", "Person", "Person"));
        userStats.addRelation(new RelationDescriptor("Person", "knows", "one-to-one"));
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(gson.fromJson(gson.toJson(userStats), GraphDescriptor.class)), gson.toJson(userStats));

    }
}
