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

package org.apache.geaflow.shuffle.network.protocol;

import com.google.common.base.Preconditions;

public enum MessageType {

    ERROR_RESPONSE(1),
    FETCH_SLICE_REQUEST(2),
    FETCH_SLICE_RESPONSE(3),
    FETCH_BATCH_REQUEST(4),
    CLOSE_CONNECTION(5),
    CANCEL_CONNECTION(6),
    ADD_CREDIT_REQUEST(7);

    private final byte id;

    MessageType(int id) {
        Preconditions.checkArgument(id < 128, "Cannot have more than 128 message types");
        this.id = (byte) id;
    }

    public static MessageType decode(byte id) {
        switch (id) {
            case 1:
                return ERROR_RESPONSE;
            case 2:
                return FETCH_SLICE_REQUEST;
            case 3:
                return FETCH_SLICE_RESPONSE;
            case 4:
                return FETCH_BATCH_REQUEST;
            case 5:
                return CLOSE_CONNECTION;
            case 6:
                return CANCEL_CONNECTION;
            case 7:
                return ADD_CREDIT_REQUEST;
            default:
                throw new IllegalArgumentException("unrecognized MessageType:" + id);
        }
    }

    public byte getId() {
        return id;
    }

}
