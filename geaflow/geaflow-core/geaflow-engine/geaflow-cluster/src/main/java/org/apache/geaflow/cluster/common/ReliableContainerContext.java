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

package org.apache.geaflow.cluster.common;

import java.io.Serializable;
import org.apache.geaflow.common.config.Configuration;

public abstract class ReliableContainerContext implements IReliableContext, Serializable {

    protected final int id;
    protected final String name;
    protected final Configuration config;
    protected boolean isRecover;

    public ReliableContainerContext(int id, String name, Configuration config) {
        this.id = id;
        this.name = name;
        this.config = config;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Configuration getConfig() {
        return config;
    }

    public boolean isRecover() {
        return isRecover;
    }

    @Override
    public synchronized void checkpoint(IReliableContext.IReliableContextCheckpointFunction function) {
        function.doCheckpoint(this);
    }
}
