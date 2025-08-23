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

package org.apache.geaflow.shuffle.pipeline.buffer;

import org.apache.geaflow.shuffle.pipeline.channel.LocalInputChannel;
import org.apache.geaflow.shuffle.pipeline.slice.SequenceSliceReader;

/**
 * Message consumed by channels ({@link LocalInputChannel}
 * and {@link SequenceSliceReader}).
 */
public class PipeChannelBuffer {

    private final PipeBuffer buffer;
    // Indicate the availability of message in PipeSlice.
    private final boolean moreAvailable;

    public PipeChannelBuffer(PipeBuffer buffer, boolean moreAvailable) {
        this.buffer = buffer;
        this.moreAvailable = moreAvailable;
    }

    public PipeBuffer getBuffer() {
        return buffer;
    }

    public boolean moreAvailable() {
        return moreAvailable;
    }

}
