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

package org.apache.geaflow.infer.exchange.serialize;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class ExceptionConstructor implements IObjectConstructor {

    private static final String PYTHON_EXCEPTION_TYPE = "pythonExceptionType";

    private static final String MODULE_SUFFIX = ".";

    private static final String BRACKETS_LEFT = "[";

    private static final String BRACKETS_RIGHT = "]";

    private final Class<?> type;

    private final String pythonExceptionType;

    public ExceptionConstructor(Class<?> type, String module, String name) {
        if (module != null) {
            pythonExceptionType = module + MODULE_SUFFIX + name;
        } else {
            pythonExceptionType = name;
        }
        this.type = type;
    }

    public Object construct(Object[] args) {
        try {
            if (pythonExceptionType != null) {
                if (args == null || args.length == 0) {
                    args = new String[]{BRACKETS_LEFT + pythonExceptionType + BRACKETS_RIGHT};
                } else {
                    String msg = BRACKETS_LEFT + pythonExceptionType + BRACKETS_RIGHT + args[0];
                    args = new String[]{msg};
                }
            }
            Class<?>[] paramTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; ++i) {
                paramTypes[i] = args[i].getClass();
            }
            Constructor<?> cons = type.getConstructor(paramTypes);
            Object ex = cons.newInstance(args);

            try {
                Field prop = ex.getClass().getField(PYTHON_EXCEPTION_TYPE);
                prop.set(ex, pythonExceptionType);
            } catch (NoSuchFieldException e) {
                throw new GeaflowRuntimeException(e);
            }
            return ex;
        } catch (Exception x) {
            throw new PickleException("problem construction object: " + x);
        }
    }
}
