
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

package org.apache.geaflow.dsl.common.compile;

import java.io.Serializable;
import java.util.Objects;

public class FunctionInfo implements Serializable {

    private final String instanceName;

    private final String functionName;

    public FunctionInfo(String instanceName, String functionName) {
        this.instanceName = instanceName;
        this.functionName = functionName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public String toString() {
        return instanceName + "." + functionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FunctionInfo)) {
            return false;
        }
        FunctionInfo that = (FunctionInfo) o;
        return Objects.equals(instanceName, that.instanceName) && Objects.equals(functionName,
            that.functionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceName, functionName);
    }
}
