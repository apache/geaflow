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

package org.apache.geaflow.model.graph.message;

import java.util.NoSuchElementException;

public class DefaultGraphMessage<K, MESSAGE> implements IGraphMessage<K, MESSAGE> {

    private final K targetVId;
    private MESSAGE message;

    public DefaultGraphMessage(K targetVId, MESSAGE message) {
        this.targetVId = targetVId;
        this.message = message;
    }

    public MESSAGE getMessage() {
        return this.message;
    }

    @Override
    public K getTargetVId() {
        return this.targetVId;
    }

    @Override
    public boolean hasNext() {
        return this.message != null;
    }

    @Override
    public MESSAGE next() {
        if (this.message == null) {
            throw new NoSuchElementException();
        }
        MESSAGE msg = this.message;
        this.message = null;
        return msg;
    }

    @Override
    public String toString() {
        return "DefaultGraphMessage{" + "targetVId=" + targetVId + ", message=" + message + '}';
    }

}
