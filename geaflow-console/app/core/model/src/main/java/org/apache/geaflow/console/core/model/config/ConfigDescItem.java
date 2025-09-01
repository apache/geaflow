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
import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

@Getter
public class ConfigDescItem {

    @JSONField(serialize = false, deserialize = false)
    private final Field field;

    private final String key;

    private final String comment;

    @JSONField(serialize = false, deserialize = false)
    private final boolean jsonIgnore;

    private final GeaflowConfigType type;

    private boolean required;

    private Object defaultValue;

    private boolean masked;

    @JSONField(serialize = false, deserialize = false)
    private ConfigValueBehavior behavior = ConfigValueBehavior.NESTED;

    @JSONField(serialize = false, deserialize = false)
    private GeaflowConfigDesc innerConfigDesc;

    public ConfigDescItem(Field field) {
        field.setAccessible(true);

        final Class<?> clazz = field.getType();
        final GeaflowConfigKey keyTag = field.getAnnotation(GeaflowConfigKey.class);
        final GeaflowConfigValue valueTag = field.getAnnotation(GeaflowConfigValue.class);
        Preconditions.checkNotNull(keyTag, "GeaflowConfigKey annotation is required");

        this.field = field;
        this.key = StringUtils.trimToNull(keyTag.value());
        this.comment = StringUtils.trimToNull(keyTag.comment());
        this.jsonIgnore = keyTag.jsonIgnore();
        this.type = GeaflowConfigType.of(clazz);

        if (valueTag != null) {
            this.required = valueTag.required();

            String str = StringUtils.trimToNull(valueTag.defaultValue());
            if (StringUtils.isNotBlank(str)) {
                this.defaultValue = String.class.equals(clazz) ? str : JSON.parseObject(str, clazz);
            }

            this.masked = valueTag.masked();
            this.behavior = valueTag.behavior();
            if (!ConfigValueBehavior.NESTED.equals(this.behavior) && !GeaflowConfigType.CONFIG.equals(this.type)) {
                throw new GeaflowException("Only CONFIG type field can use NESTED behavior on key {}", this.key);
            }
        }

        if (GeaflowConfigClass.class.isAssignableFrom(clazz)) {
            this.innerConfigDesc = ConfigDescFactory.getOrRegister((Class<? extends GeaflowConfigClass>) clazz);
        }
    }

    public String getComment() {
        return I18nUtil.getMessage(comment);
    }
}
