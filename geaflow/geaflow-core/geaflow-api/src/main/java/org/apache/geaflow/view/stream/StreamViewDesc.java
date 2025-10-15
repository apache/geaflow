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

package org.apache.geaflow.view.stream;

import java.util.HashMap;
import org.apache.geaflow.api.partition.IPartition;
import org.apache.geaflow.view.IViewDesc;

public class StreamViewDesc implements IViewDesc {

    private String viewName;
    private int shardNum;
    private BackendType backend;
    private IPartition partitioner;
    private HashMap props;

    public StreamViewDesc(String viewName, int shardNum, BackendType backend) {
        this.viewName = viewName;
        this.shardNum = shardNum;
        this.backend = backend;
    }

    public StreamViewDesc(String viewName, int shardNum, BackendType backend,
                          IPartition partitioner, HashMap props) {
        this(viewName, shardNum, backend);
        this.partitioner = partitioner;
        this.props = props;
    }

    @Override
    public String getName() {
        return this.viewName;
    }

    @Override
    public int getShardNum() {
        return this.shardNum;
    }

    @Override
    public DataModel getDataModel() {
        return DataModel.TABLE;
    }

    @Override
    public BackendType getBackend() {
        return backend;
    }

    @Override
    public HashMap getViewProps() {
        return this.props;
    }
}
