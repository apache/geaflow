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

package org.apache.geaflow.state.manage;

import org.apache.geaflow.utils.keygroup.KeyGroup;

public class LoadOption {

    private KeyGroup keyGroup;
    private long checkPointId;
    private LoadEnum loadEnum = LoadEnum.REMOTE_TO_DISK;

    private LoadOption() {

    }

    public static LoadOption of() {
        return new LoadOption();
    }

    public LoadOption withKeyGroup(KeyGroup keyGroup) {
        this.keyGroup = keyGroup;
        return this;
    }

    public LoadOption withLoadEnum(LoadEnum loadEnum) {
        this.loadEnum = loadEnum;
        return this;
    }

    public LoadOption withCheckpointId(long checkpointId) {
        this.checkPointId = checkpointId;
        return this;
    }

    public KeyGroup getKeyGroup() {
        return keyGroup;
    }

    public LoadEnum getLoadEnum() {
        return loadEnum;
    }

    public long getCheckPointId() {
        return checkPointId;
    }

    public enum LoadEnum {
        // Download remote files to local disk.
        REMOTE_TO_DISK,
        // Download remote files to local disk and load to memory.
        REMOTE_TO_MEM
    }
}
