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

package org.apache.geaflow.cluster.resourcemanager;

import java.io.Serializable;
import java.util.List;

public class ResourceInfo implements Serializable {

    private String resourceId;
    private List<WorkerInfo> workers;

    public ResourceInfo(String resourceId, List<WorkerInfo> workers) {
        this.resourceId = resourceId;
        this.workers = workers;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public void setWorkers(List<WorkerInfo> workers) {
        this.workers = workers;
    }

    public String getResourceId() {
        return resourceId;
    }

    public List<WorkerInfo> getWorkers() {
        return workers;
    }

    @Override
    public String toString() {
        return "ResourceInfo{"
            + "resourceId=" + resourceId
            + ", workers=" + workers
            + '}';
    }
}
