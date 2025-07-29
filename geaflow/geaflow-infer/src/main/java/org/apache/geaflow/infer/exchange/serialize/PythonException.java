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

import java.util.HashMap;
import java.util.List;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class PythonException extends GeaflowRuntimeException {

    private static final long serialVersionUID = 4884843316742683086L;

    private static final String TRACEBACK = "_pyroTraceback";

    public String errorTraceback;

    public String pythonExceptionType;

    public PythonException(String message, Throwable cause) {
        super(message, cause);
    }

    public PythonException(String message) {
        super(message);
    }

    public PythonException(Throwable cause) {
        super(cause);
    }

    public void setState(HashMap<String, Object> args) {
        Object tb = args.get(TRACEBACK);
        if (tb instanceof List) {
            StringBuilder sb = new StringBuilder();
            for (Object line : (List<?>) tb) {
                sb.append(line);
            }
            errorTraceback = sb.toString();
        } else {
            errorTraceback = (String) tb;
        }
    }
}
