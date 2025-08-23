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

package org.apache.geaflow.state.strategy.accessor;

import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.graph.StateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessorBuilder {

    public static final Logger LOGGER = LoggerFactory.getLogger(AccessorBuilder.class);

    public static IAccessor getAccessor(DataModel dataModel, StateMode stateMode) {
        switch (dataModel) {
            case STATIC_GRAPH:
                return getStaticGraphAccessor(stateMode);
            case DYNAMIC_GRAPH:
                return getDynamicGraphAccessor(stateMode);
            case KV:
                return getKVAccessor(stateMode);
            case KList:
                return getKListAccessor(stateMode);
            case KMap:
                return getKMapAccessor(stateMode);
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    private static IAccessor getStaticGraphAccessor(StateMode stateMode) {
        switch (stateMode) {
            case RW:
                return new RWStaticGraphAccessor<>();
            case RDONLY:
                return new ReadOnlyStaticGraphAccessor<>();
            case COW:
                return new COWGraphAccessor<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    private static IAccessor getDynamicGraphAccessor(StateMode stateMode) {
        switch (stateMode) {
            case RW:
                return new RWDynamicGraphAccessor<>();
            case RDONLY:
                return new ReadOnlyDynamicGraphAccessor<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    private static IAccessor getKVAccessor(StateMode stateMode) {
        switch (stateMode) {
            case RW:
                return new RWKeyValueAccessor<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    private static IAccessor getKListAccessor(StateMode stateMode) {
        switch (stateMode) {
            case RW:
                return new RWKeyListAccessor<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    private static IAccessor getKMapAccessor(StateMode stateMode) {
        switch (stateMode) {
            case RW:
                return new RWKeyMapAccessor<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }
}
