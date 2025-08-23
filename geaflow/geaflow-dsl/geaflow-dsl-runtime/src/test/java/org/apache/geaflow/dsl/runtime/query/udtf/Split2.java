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

package org.apache.geaflow.dsl.runtime.query.udtf;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDTF;

@Description(name = "split2", description = "")
public class Split2 extends UDTF {
    private String columnDelimiter = ",";
    private String lineDelimiter = "\n";

    public void eval(String data) {
        evalInternal(data, columnDelimiter, lineDelimiter);
    }

    public void eval(String data, String columnDelimiter) {
        evalInternal(data, columnDelimiter, lineDelimiter);
    }

    public void eval(String data, String columnDelimiter, String lineDelimiter) {
        evalInternal(data, columnDelimiter, lineDelimiter);
    }

    private void evalInternal(String data, String columnDelimiter, String lineDelimiter) {
        String[] rows = StringUtils.split(data, lineDelimiter);
        for (String row : rows) {
            String[] split = StringUtils.split(row, columnDelimiter);
            collect(split);
        }
    }

    @Override
    public List<Class<?>> getReturnType(List<Class<?>> paramTypes, List<String> udtfReturnFields) {

        List<Class<?>> clazzs = Lists.newArrayList();

        if (udtfReturnFields == null) {
            clazzs.add(String.class);
            clazzs.add(String.class);
            return clazzs;
        }

        for (int i = 0; i < udtfReturnFields.size(); i++) {
            clazzs.add(String.class);
        }
        return clazzs;
    }
}
