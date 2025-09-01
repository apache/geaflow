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

package org.apache.geaflow.utils;

import java.io.Serializable;

public class TicToc implements Serializable {

    private long start = 0L;

    public void tic() {
        this.start = System.currentTimeMillis();
    }

    public long toc() {
        long end = System.currentTimeMillis();
        long duration = end - this.start;
        this.start = end;
        return duration;
    }

    public void ticNano() {
        start = System.nanoTime();
    }

    public long tocNano() {
        long end = System.nanoTime();
        long duration = end - this.start;
        if (duration < 0) {
            return -1;
        }
        this.start = end;
        return duration;
    }

}
