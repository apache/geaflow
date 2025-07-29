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

package org.apache.geaflow.cluster.fetcher;

public class FetchRequest implements IFetchRequest {

    private final int taskId;
    private final long windowId;
    private final long windowCount;

    public FetchRequest(int taskId, long windowId, long windowCount) {
        this.taskId = taskId;
        this.windowId = windowId;
        this.windowCount = windowCount;
    }

    @Override
    public int getTaskId() {
        return this.taskId;
    }

    public long getWindowId() {
        return this.windowId;
    }

    public long getWindowCount() {
        return this.windowCount;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.FETCH;
    }

}
