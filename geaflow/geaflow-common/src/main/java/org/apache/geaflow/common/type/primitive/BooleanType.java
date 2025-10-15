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

package org.apache.geaflow.common.type.primitive;

import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

public class BooleanType implements IType<Boolean> {

    public static final BooleanType INSTANCE = new BooleanType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_BOOLEAN;
    }

    @Override
    public Class<Boolean> getTypeClass() {
        return Boolean.class;
    }

    @Override
    public byte[] serialize(Boolean obj) {
        if (obj == null) {
            return null;
        }
        if (Boolean.TRUE.equals(obj)) {
            return new byte[]{1};
        }
        if (Boolean.FALSE.equals(obj)) {
            return new byte[]{0};
        }
        String msg = "illegal boolean value: " + obj;
        throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
    }

    @Override
    public Boolean deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes[0] == 1) {
            return Boolean.TRUE;
        }
        if (bytes[0] == 0) {
            return Boolean.FALSE;
        }
        String msg = "illegal boolean value: " + bytes[0];
        throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
    }

    @Override
    public int compare(Boolean a, Boolean b) {
        return Types.compare(a, b);
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }
}
