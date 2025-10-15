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

package org.apache.geaflow.cluster.client;

import java.util.concurrent.CompletableFuture;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.rpc.proto.Driver;
import org.apache.geaflow.rpc.proto.Driver.PipelineRes;
import org.junit.Assert;
import org.testng.annotations.Test;

public class PipelineResultTest {

    @Test
    public void testResult() {
        CompletableFuture<PipelineRes> res = new CompletableFuture<>();
        res.complete(Driver.PipelineRes.newBuilder().setPayload(RpcMessageEncoder.encode("test")).build());
        PipelineResult result = new PipelineResult(res);
        Assert.assertEquals("test", result.get());
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class,
        expectedExceptionsMessageRegExp = ".*get pipeline result error.*")
    public void testNotHasResult() {
        CompletableFuture<PipelineRes> res = new CompletableFuture<>();
        res.completeExceptionally(new Throwable());
        PipelineResult result = new PipelineResult(res);
        Assert.assertTrue(result.isSuccess());
    }

}
