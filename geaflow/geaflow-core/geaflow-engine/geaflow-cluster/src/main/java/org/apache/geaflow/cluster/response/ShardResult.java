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

import java.util.List;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.message.ISliceMeta;

public class ShardResult implements IResult<ISliceMeta> {

    /**
     * Use edge id of output info to identify the result.
     */
    private int id;
    private OutputType outputType;
    private List<ISliceMeta> slices;
    private long recordNum;
    private long recordBytes;

    public ShardResult(int id, OutputType outputType, List<ISliceMeta> slices) {
        this.id = id;
        this.outputType = outputType;
        this.slices = slices;
        if (slices != null) {
            for (ISliceMeta sliceMeta : slices) {
                recordNum += sliceMeta.getRecordNum();
                recordBytes += sliceMeta.getEncodedSize();
            }
        }
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public List<ISliceMeta> getResponse() {
        return slices;
    }

    public long getRecordNum() {
        return recordNum;
    }

    public long getRecordBytes() {
        return recordBytes;
    }

    @Override
    public OutputType getType() {
        return outputType;
    }
}
