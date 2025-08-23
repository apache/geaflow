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

package org.apache.geaflow.dsl.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.sqlnode.SqlCreateFunction;

public class GeaFlowFunction implements Serializable {

    public enum FunctionType {

        /**
         * User-defined scalar function.
         */
        UDF,

        /**
         * User-defined table function.
         */
        UDTF,

        /**
         * User-defined aggregate function.
         */
        UDAF,

        /**
         * User-defined graph algorithm.
         */
        UDGA
    }

    /**
     * Function name.
     */
    private final String name;

    private final List<String> clazz;

    private final String url;

    private final boolean ifNotExists;

    public static GeaFlowFunction toFunction(SqlCreateFunction function) {
        // Extract function name
        String functionName = ((SqlIdentifier) function.getFunctionName()).getSimple();
        String className = function.getClassName();
        String url = function.getUsingPath();
        return new GeaFlowFunction(functionName, Lists.newArrayList(className), url, function.ifNotExists());
    }


    public static GeaFlowFunction of(Class functionClazz) {
        Description description = (Description) functionClazz.getAnnotation(Description.class);

        Preconditions.checkState(description != null,
            "missing Description annotation for udf " + functionClazz);

        Preconditions.checkArgument(!description.name().contains(","),
            "bad udf name " + description.name() + " in " + functionClazz);

        return new GeaFlowFunction(description.name(), functionClazz.getName(), false);
    }

    public static GeaFlowFunction of(String name, Class reflectClass) {
        return new GeaFlowFunction(name, reflectClass.getName(), false);
    }

    public static GeaFlowFunction of(String name, List<String> classNames) {
        return of(name, classNames, null);
    }

    public static GeaFlowFunction of(String name, List<String> classNames, String url) {
        return new GeaFlowFunction(name, classNames, url, false);
    }

    private GeaFlowFunction(String name, String clazz, boolean ifNotExists) {
        this(name, Lists.newArrayList(clazz), null, ifNotExists);
    }

    private GeaFlowFunction(String name, List<String> clazz, String url, boolean ifNotExists) {
        this.name = name;
        this.clazz = clazz;
        this.url = url;
        this.ifNotExists = ifNotExists;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getName() {
        return name;
    }

    public List<String> getClazz() {
        return clazz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GeaFlowFunction)) {
            return false;
        }
        GeaFlowFunction that = (GeaFlowFunction) o;
        return Objects.equals(name, that.name) && Objects.equals(clazz, that.clazz)
            && Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, clazz, url);
    }

    @Override
    public String toString() {
        return "GeaFlowFunction{"
            + "name='" + name + '\''
            + ", clazz=" + clazz
            + ", url='" + url + '\''
            + '}';
    }

    public String getUrl() {
        return url;
    }
}
