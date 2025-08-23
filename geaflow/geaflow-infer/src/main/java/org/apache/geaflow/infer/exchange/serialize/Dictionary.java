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

public class Dictionary extends HashMap<String, Object> {

    private static final long serialVersionUID = 6157715596627049511L;

    private static final String CLASS_KEY = "__class__";

    private static final String CLASS_NAME_REGEX = ".";

    private final String className;

    public Dictionary(String moduleName, String classname) {
        if (moduleName == null) {
            this.className = classname;
        } else {
            this.className = moduleName + CLASS_NAME_REGEX + classname;
        }

        this.put(CLASS_KEY, this.className);
    }

    public void setState(HashMap<String, Object> values) {
        this.clear();
        this.put(CLASS_KEY, this.className);
        this.putAll(values);
    }

    public String getClassName() {
        return this.className;
    }
}
