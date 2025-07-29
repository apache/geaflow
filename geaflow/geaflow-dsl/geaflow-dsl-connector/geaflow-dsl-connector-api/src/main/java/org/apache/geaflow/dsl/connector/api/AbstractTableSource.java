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

package org.apache.geaflow.dsl.connector.api;


import java.io.IOException;
import java.util.Optional;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.TimeFetchWindow;

public abstract class AbstractTableSource implements TableSource {

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo) throws IOException {
        switch (windowInfo.getType()) {
            case ALL_WINDOW:
                return fetch(partition, startOffset, (AllFetchWindow) windowInfo);
            case SIZE_TUMBLING_WINDOW:
                return fetch(partition, startOffset, (SizeFetchWindow) windowInfo);
            case FIXED_TIME_TUMBLING_WINDOW:
                return fetch(partition, startOffset, (TimeFetchWindow) windowInfo);
            default:
                throw new GeaFlowDSLException("Not support window type:{}", windowInfo.getType());
        }
    }


    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, AllFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, SizeFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, TimeFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

}
