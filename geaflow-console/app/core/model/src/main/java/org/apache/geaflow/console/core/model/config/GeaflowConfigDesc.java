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
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.geaflow.console.common.util.ReflectUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

@Getter
public class GeaflowConfigDesc {

    private final Class<? extends GeaflowConfigClass> clazz;

    private final List<ConfigDescItem> items;

    public GeaflowConfigDesc(Class<? extends GeaflowConfigClass> clazz) {
        this.clazz = clazz;
        this.items = parseItems(clazz);
    }

    private static List<ConfigDescItem> parseItems(Class<? extends GeaflowConfigClass> clazz) {
        List<Field> fields = ReflectUtil.getFields(clazz, GeaflowConfigClass.class,
            f -> f.getAnnotation(GeaflowConfigKey.class) != null);
        return fields.stream().map(ConfigDescItem::new).collect(Collectors.toList());
    }

    public void validateConfig(GeaflowConfig config) {
        for (ConfigDescItem item : items) {
            String key = item.getKey();
            Object value = config.get(key);

            if (value != null) {
                GeaflowConfigType valueType = GeaflowConfigType.of(value.getClass());
                GeaflowConfigType expectedType =
                    ConfigValueBehavior.JSON.equals(item.getBehavior()) ? GeaflowConfigType.STRING : item.getType();

                if (!expectedType.equals(valueType)) {
                    try {
                        if (GeaflowConfigType.CONFIG.equals(item.getType())) {
                            throw new GeaflowException("Type [CONFIG] compatible check is not allowed");
                        }
                        JSON.parseObject(JSON.toJSONString(value), item.getField().getType());

                    } catch (Exception e) {
                        throw new GeaflowException("Config key {} type {} not allowed, expected {}", key, valueType,
                            expectedType, e);
                    }
                }

                if (GeaflowConfigType.CONFIG.equals(item.getType())) {
                    validateInnerValue(config, item, value);
                }

            } else {
                if (GeaflowConfigType.CONFIG.equals(item.getType())) {
                    validateInnerRequired(config, item);

                } else {
                    if (item.isRequired()) {
                        throw new GeaflowException("Config key {} is required", key);
                    }
                }
            }
        }
    }

    private void validateInnerValue(GeaflowConfig config, ConfigDescItem item, Object value) {
        ConfigValueBehavior behavior = item.getBehavior();
        switch (behavior) {
            case NESTED:
                item.getInnerConfigDesc().validateConfig(new GeaflowConfig(value));
                break;
            case FLATTED:
                item.getInnerConfigDesc().validateConfig(config);
                break;
            case JSON:
                item.getInnerConfigDesc().validateConfig(new GeaflowConfig(JSON.parseObject((String) value)));
                break;
            default:
                throw new GeaflowException("Unsupported config value behavior {}", behavior);
        }
    }

    private void validateInnerRequired(GeaflowConfig config, ConfigDescItem item) {
        String key = item.getKey();
        ConfigValueBehavior behavior = item.getBehavior();
        switch (behavior) {
            case NESTED:
            case JSON:
                if (item.isRequired()) {
                    throw new GeaflowException("Config key {} is required", key);
                }
                break;
            case FLATTED:
                item.getInnerConfigDesc().validateConfig(config);
                break;
            default:
                throw new GeaflowException("Unsupported config value behavior {}", behavior);
        }
    }

}
