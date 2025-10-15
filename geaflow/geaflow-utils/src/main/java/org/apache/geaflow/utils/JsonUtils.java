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

package org.apache.geaflow.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class JsonUtils {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static Map<String, String> parseJson2map(String str) {
        try {
            JsonNode jsonNode = MAPPER.readTree(str);
            Map<String, String> map = new HashMap<>();
            Iterator<String> fieldNames = jsonNode.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                JsonNode value = jsonNode.get(key);
                if (value instanceof ContainerNode) {
                    map.put(key, value.toString());
                } else {
                    map.put(key, value.asText());
                }
            }
            return map;
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public static String toJsonString(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
