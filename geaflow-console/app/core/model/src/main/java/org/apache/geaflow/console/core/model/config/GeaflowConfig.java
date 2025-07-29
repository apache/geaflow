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

package org.apache.geaflow.console.core.model.config;

import com.alibaba.fastjson.JSON;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

@Getter
@NoArgsConstructor
public class GeaflowConfig extends LinkedHashMap<String, Object> {

    public GeaflowConfig(Object map) {
        putAll((Map<String, Object>) map);
    }

    public Map<String, String> toStringMap() {
        Map<String, String> stringMap = new LinkedHashMap<>();
        forEach((k, v) -> stringMap.put(k, v instanceof GeaflowConfig ? JSON.toJSONString(v) : v.toString()));
        return stringMap;
    }

    public final <T extends GeaflowConfigClass> T parse(Class<T> clazz, boolean fillWithDefault) {
        if (Modifier.isAbstract(clazz.getModifiers())) {
            throw new GeaflowException("Config field abstract type {} can't be parsed", clazz);
        }

        GeaflowConfigDesc configDesc = ConfigDescFactory.getOrRegister(clazz);
        configDesc.validateConfig(this);

        try {
            T instance = clazz.newInstance();

            Set<String> usedKeys = new HashSet<>();
            for (ConfigDescItem item : configDesc.getItems()) {
                String key = item.getKey();
                Object value = this.get(key);
                if (value == null && fillWithDefault) {
                    value = item.getDefaultValue();
                }
                if (GeaflowConfigType.CONFIG.equals(item.getType())) {
                    value = innerParse(item, value);

                } else {
                    // use json do auto convert
                    if (value != null) {
                        Class<?> fieldType = item.getField().getType();
                        if (!fieldType.isAssignableFrom(value.getClass())) {
                            value = JSON.parseObject(JSON.toJSONString(value), fieldType);
                        }
                    }
                }

                usedKeys.add(key);
                item.getField().set(instance, value);
            }

            this.forEach((k, v) -> {
                if (!usedKeys.contains(k)) {
                    instance.getExtendConfig().put(k, v);
                }
            });

            return instance;

        } catch (Exception e) {
            throw new GeaflowException("Parse config with {} failed", clazz.getSimpleName(), e);
        }
    }

    public final <T extends GeaflowConfigClass> T parse(Class<T> clazz) {
        return parse(clazz, false);
    }

    private Object innerParse(ConfigDescItem item, Object value) {
        ConfigValueBehavior behavior = item.getBehavior();
        Class<? extends GeaflowConfigClass> innerConfigClass = item.getInnerConfigDesc().getClazz();
        switch (behavior) {
            case NESTED:
                return new GeaflowConfig(value).parse(innerConfigClass);
            case FLATTED:
                throw new GeaflowException("Unsupported FLATTED behavior for {}", item.getKey());
            case JSON:
                return new GeaflowConfig(JSON.parseObject((String) value)).parse(innerConfigClass);
            default:
                throw new GeaflowException("Unsupported config value behavior {}", behavior);
        }
    }
}
