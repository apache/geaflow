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

package org.apache.geaflow.cluster.response;

import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.shuffle.desc.OutputType;

public class ResponseResult implements IResult<Object>, Serializable {

    private int collectId;
    private OutputType outputType;
    private List<Object> responses;

    public ResponseResult(int collectId, OutputType outputType, List<Object> responses) {
        this.collectId = collectId;
        this.outputType = outputType;
        this.responses = responses;
    }

    @Override
    public int getId() {
        return collectId;
    }

    @Override
    public List<Object> getResponse() {
        return responses;
    }

    @Override
    public OutputType getType() {
        return outputType;
    }

}

