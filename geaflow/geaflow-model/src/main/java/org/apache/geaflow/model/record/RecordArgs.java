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

package org.apache.geaflow.model.record;

public class RecordArgs {

    private long windowId;
    private String name;

    public RecordArgs() {
        this.windowId = 0;
        this.name = "";
    }

    public RecordArgs(long windowId) {
        this.windowId = windowId;
    }

    public RecordArgs(long windowId, String name) {
        this.windowId = windowId;
        this.name = name;
    }

    public long getWindowId() {
        return windowId;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "RecordArgs{" + "windowId=" + windowId + ", name='" + name + '\'' + '}';
    }

    public enum GraphRecordNames {
        Vertex, Edge, Request, Message, Aggregate;
    }
}
