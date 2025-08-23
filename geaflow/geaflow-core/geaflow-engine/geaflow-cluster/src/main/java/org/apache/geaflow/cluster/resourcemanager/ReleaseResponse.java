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

public class ReleaseResponse {

    private final String releaseId;
    private final boolean success;
    private final String msg;

    private ReleaseResponse(String releaseId, boolean success, String msg) {
        this.releaseId = releaseId;
        this.success = success;
        this.msg = msg;
    }

    public String getReleaseId() {
        return this.releaseId;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public String getMsg() {
        return this.msg;
    }

    public static ReleaseResponse success(String releaseId) {
        return new ReleaseResponse(releaseId, true, null);
    }

    public static ReleaseResponse fail(String releaseId, String msg) {
        return new ReleaseResponse(releaseId, false, msg);
    }

}
