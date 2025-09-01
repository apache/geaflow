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

package org.apache.geaflow.dsl.runtime.traversal.message;

import org.apache.geaflow.dsl.common.data.VirtualId;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepJoinOperator;

public enum MessageType {
    /**
     * The message type for {@link IPathMessage} which is the path message send to the vertex.
     */
    PATH,
    /**
     * The message type for {@link EODMessage} which is end-of-data message sending to the next
     * by the step operator when finished processing all the data.
     */
    EOD,
    /**
     * The message type for {@link ParameterRequestMessage} which is the request message with the parameters.
     */
    PARAMETER_REQUEST,
    /**
     * The path message type for {@link JoinPathMessage} which  for the
     * {@link StepJoinOperator}.
     */
    JOIN_PATH,
    /**
     * The message type for {@link KeyGroupMessage} which is the message send to the
     * {@link VirtualId} to do same relation operations
     * e.g. aggregate.
     */
    KEY_GROUP,
    /**
     * The message type for {@link ReturnMessage} which is the result returning from the sub-query calling.
     */
    RETURN_VALUE,
    /**
     * The message type for {@link EvolveVertexMessage} which is the message to evolve vertices in incrMatch.
     */
    EVOLVE_VERTEX;

    public SingleMessageBox createMessageBox() {
        switch (this) {
            case PATH:
                return new PathMessageBox();
            case EOD:
                return new EODMessageBox();
            case PARAMETER_REQUEST:
                return new ParameterRequestMessageBox();
            case JOIN_PATH:
                return new JoinPathMessageBox();
            case KEY_GROUP:
                return new KeyGroupMessageBox();
            case RETURN_VALUE:
                return new ReturnMessageBox();
            case EVOLVE_VERTEX:
                return new EvolveVertexMessageBox();
            default:
                throw new IllegalArgumentException("Failing to create message box for: " + this);
        }
    }
}
