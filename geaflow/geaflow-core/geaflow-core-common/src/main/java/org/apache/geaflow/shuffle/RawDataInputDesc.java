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

package org.apache.geaflow.shuffle;

import java.util.List;
import org.apache.geaflow.shuffle.desc.IInputDesc;
import org.apache.geaflow.shuffle.desc.InputType;

public class RawDataInputDesc<T> implements IInputDesc<T> {

    private final int edgeId;
    private final String edgeName;
    private final List<T> rawData;

    public RawDataInputDesc(int edgeId, String edgeName, List<T> rawData) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.rawData = rawData;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public String getName() {
        return edgeName;
    }

    @Override
    public List<T> getInput() {
        return rawData;
    }

    @Override
    public InputType getInputType() {
        return InputType.DATA;
    }
}
