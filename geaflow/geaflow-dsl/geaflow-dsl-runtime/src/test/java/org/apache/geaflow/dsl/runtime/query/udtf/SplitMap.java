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
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDTF;

@Description(name = "split_map", description = "")
public class SplitMap extends UDTF {

    private String splitChar = ",";

    public void eval(String arg0) {
        evalInternal(arg0);
    }

    public void eval(String arg0, String arg1) {
        evalInternal(arg0, arg1);
    }

    private void evalInternal(String... args) {
        if (args != null && (args.length == 1 || args.length == 2)) {
            if (args.length == 2 && StringUtils.isNotEmpty(args[1])) {
                splitChar = args[1];
            }
            String[] lines = StringUtils.split(args[0], splitChar);
            collect(new Object[]{lines[0]});
        }
    }

    @Override
    public List<Class<?>> getReturnType(List<Class<?>> paramTypes, List<String> udtfReturnFields) {

        List<Class<?>> clazzs = Lists.newArrayList();

        if (udtfReturnFields == null) {
            clazzs.add(String.class);
            return clazzs;
        }

        for (int i = 0; i < udtfReturnFields.size(); i++) {
            clazzs.add(String.class);
        }

        return clazzs;
    }
}
