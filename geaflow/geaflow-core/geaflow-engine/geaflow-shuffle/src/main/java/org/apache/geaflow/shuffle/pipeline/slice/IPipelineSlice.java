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

import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;

public interface IPipelineSlice {

    SliceId getSliceId();

    boolean isReleased();

    boolean canRelease();

    void release();

    // ------------------------------------------------------------------------
    // Produce
    // ------------------------------------------------------------------------

    boolean add(PipeBuffer recordBuffer);

    void flush();

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    PipelineSliceReader createSliceReader(long startBatchId, PipelineSliceListener listener);

    /**
     * Check whether the slice is ready to read.
     */
    boolean isReady2read();

    /**
     * Check whether the slice has next record.
     */
    boolean hasNext();

    /**
     * Poll next record from slice.
     *
     * @return
     */
    PipeBuffer next();

}
