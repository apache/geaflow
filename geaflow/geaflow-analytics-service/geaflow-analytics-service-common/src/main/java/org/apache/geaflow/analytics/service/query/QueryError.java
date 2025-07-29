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

package org.apache.geaflow.analytics.service.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class QueryError implements Externalizable {

    private static final int DEFAULT_ERROR_CODE = 0;
    private static final String DELIMITER = ":";
    private int code;
    private String name;

    public QueryError(String name, int code) {
        if (code < 0) {
            throw new GeaflowRuntimeException("query error code is negative");
        }
        this.name = name;
        this.code = code;
    }

    public QueryError(String name) {
        this.name = name;
        this.code = DEFAULT_ERROR_CODE;
    }

    public QueryError() {
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryError that = (QueryError) obj;
        return Objects.equals(this.code, that.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }

    @Override
    public String toString() {
        return name + DELIMITER + code;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(code);
        out.writeObject(name);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        code = in.readInt();
        name = (String) in.readObject();
    }
}
