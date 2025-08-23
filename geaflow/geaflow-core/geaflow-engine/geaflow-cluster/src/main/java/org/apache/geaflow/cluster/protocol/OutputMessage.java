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

package org.apache.geaflow.cluster.protocol;

import java.util.List;

public class OutputMessage<T> extends AbstractMessage<List<T>> {

    private final int targetChannel;
    private final List<T> data;
    private final boolean isBarrier;

    public OutputMessage(long windowId, int targetChannel, List<T> data) {
        super(windowId);
        this.targetChannel = targetChannel;
        this.data = data;
        this.isBarrier = data == null;
    }

    @Override
    public List<T> getMessage() {
        return this.data;
    }

    public int getTargetChannel() {
        return this.targetChannel;
    }

    public boolean isBarrier() {
        return this.isBarrier;
    }

    public static <T> OutputMessage<T> data(long windowId, int targetChannel, List<T> data) {
        return new OutputMessage<>(windowId, targetChannel, data);
    }

    public static <T> OutputMessage<T> barrier(long windowId) {
        return new OutputMessage<>(windowId, -1, null);
    }

}
