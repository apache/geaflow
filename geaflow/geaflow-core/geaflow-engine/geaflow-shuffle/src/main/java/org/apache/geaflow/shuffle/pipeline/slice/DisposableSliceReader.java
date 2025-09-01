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

package org.apache.geaflow.shuffle.pipeline.slice;

import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;

/**
 * DisposableReader poll the data from slice queue and release the slice on completion.
 */
public class DisposableSliceReader extends PipelineSliceReader {

    public DisposableSliceReader(IPipelineSlice slice, long startBatchId,
                                 PipelineSliceListener listener) {
        super(slice, startBatchId, listener);
    }

    @Override
    public boolean hasNext() {
        if (isReleased()) {
            throw new IllegalStateException("slice has been released already: " + slice.getSliceId());
        }
        return hasBatch() && slice.hasNext();
    }

    public PipeChannelBuffer next() {
        PipeBuffer record = slice.next();
        if (record == null) {
            return null;
        }
        if (!record.isData()) {
            consumedBatchId = record.getBatchId();
        }
        return new PipeChannelBuffer(record, hasNext());
    }

}
