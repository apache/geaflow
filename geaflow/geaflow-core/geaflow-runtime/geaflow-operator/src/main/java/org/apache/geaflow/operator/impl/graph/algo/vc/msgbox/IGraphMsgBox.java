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

package org.apache.geaflow.operator.impl.graph.algo.vc.msgbox;

import java.util.List;

public interface IGraphMsgBox<K, MESSAGE> {

    /**
     * Add in-message into box.
     */
    void addInMessages(K vertexId, MESSAGE message);

    /**
     * Process in-message by process function.
     */
    void processInMessage(MsgProcessFunc<K, MESSAGE> processFunc);

    /**
     * Clear in-message box.
     */
    void clearInBox();

    /**
     * Add out-message into box.
     */
    void addOutMessage(K vertexId, MESSAGE message);

    /**
     * Process out-message by process function.
     */
    void processOutMessage(MsgProcessFunc<K, MESSAGE> processFunc);

    /**
     * Clear out-message box.
     */
    void clearOutBox();

    @FunctionalInterface
    interface MsgProcessFunc<K, MESSAGE> {

        /**
         * Process messages.
         */
        void process(K vertexId, List<MESSAGE> messageList);

    }

}
