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

package org.apache.geaflow.memory;

public enum ESegmentSize {
    /**
     * 16 bytes.
     */
    S16(16, 4),
    /**
     * 32 bytes.
     */
    S32(32, 5),
    /**
     * 64 bytes.
     */
    S64(64, 6),
    /**
     * 128 bytes.
     */
    S128(128, 7),
    /**
     * 256 bytes.
     */
    S256(256, 8),
    /**
     * 512 bytes.
     */
    S512(512, 9),
    /**
     * 1 KB.
     */
    S1024(1024, 10),
    /**
     * 2 KB.
     */
    S2048(2048, 11),
    /**
     * 4 KB.
     */
    S4096(4096, 12),
    /**
     * 8 KB.
     */
    S8192(8192, 13),
    /**
     * 16 KB.
     */
    S16384(16384, 14),
    /**
     * 32 KB.
     */
    S32768(32768, 15),
    /**
     * 64 KB.
     */
    S65536(65536, 16),
    /**
     * 128 KB.
     */
    S131072(131072, 17),
    /**
     * 256 KB.
     */
    S262144(262144, 18),
    /**
     * 512 KB.
     */
    S524288(524288, 19),
    /**
     * 1 MB.
     */
    S1048576(1048576, 20),
    /**
     * 2 MB.
     */
    S2097152(2097152, 21),
    /**
     * 4 MB.
     */
    S4194304(4194304, 22),
    /**
     * 8 MB.
     */
    S8388608(8388608, 23),
    /**
     * 16 MB.
     */
    S16777216(16777216, 24);

    public static ESegmentSize[] upValues = new ESegmentSize[]{
        S16,
        S32,
        S64,
        S128,
        S256,
        S512,
        S1024,
        S2048,
        S4096,
        S8192,
        S16384,
        S32768,
        S65536,
        S131072,
        S262144,
        S524288,
        S1048576,
        S2097152,
        S4194304,
        S8388608,
        S16777216
    };

    private static int baseSigPos = upValues[0].mostSigPos;

    private final int size;
    private final int mostSigPos;

    ESegmentSize(int size, int mostSigPos) {
        this.size = size;
        this.mostSigPos = mostSigPos;
    }

    public static ESegmentSize valueOf(int len) {
        int n = (int) (Math.log(len) / Math.log(2));
        return upValues[n - smallest().mostSigPos];
    }

    public static ESegmentSize largest() {
        return upValues[upValues.length - 1];
    }

    public static ESegmentSize smallest() {
        return upValues[0];
    }

    public int size() {
        return this.size;
    }

    public int index() {
        return mostSigPos - baseSigPos;
    }

}
