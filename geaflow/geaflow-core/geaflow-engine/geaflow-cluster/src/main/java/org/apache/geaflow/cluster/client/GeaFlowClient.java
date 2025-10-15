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

import java.io.Serializable;
import org.apache.geaflow.env.ctx.IEnvironmentContext;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;

public class GeaFlowClient implements Serializable {

    private IClusterClient clusterClient;
    private IPipelineClient pipelineClient;

    public void init(IEnvironmentContext environmentContext, IClusterClient clusterClient) {
        this.clusterClient = clusterClient;
        this.clusterClient.init(environmentContext);
    }

    public void startCluster() {
        this.pipelineClient = this.clusterClient.startCluster();
    }

    public IPipelineResult submit(Pipeline pipeline) {
        return this.pipelineClient.submit(pipeline);
    }

    public void shutdown() {
        if (this.pipelineClient != null) {
            this.pipelineClient.close();
        }
        this.clusterClient.shutdown();
    }

}

