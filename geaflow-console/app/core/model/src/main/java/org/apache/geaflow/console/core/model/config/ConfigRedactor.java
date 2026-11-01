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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.common.util.type.GeaflowLLMType;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.job.config.CodeJobConfigClass;
import org.apache.geaflow.console.core.model.job.config.JobConfigClass;
import org.apache.geaflow.console.core.model.job.config.ServeJobConfigClass;
import org.apache.geaflow.console.core.model.llm.LLMConfigArgsClass;
import org.apache.geaflow.console.core.model.llm.OpenAIConfigArgsClass;

/** HTTP presentation policy. Never apply this policy to engine or persisted configuration. */
public final class ConfigRedactor {

    public static final String MASK = "******";

    private static final Pattern EMBEDDED_CREDENTIAL = Pattern.compile(
        "(?i)(://[^/\\s]*@|(?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key)\\s*[=:])");

    private ConfigRedactor() {
    }

    public static GeaflowConfigDesc pluginDesc(GeaflowPluginType type) {
        return type == null || type == GeaflowPluginType.None ? null : ConfigDescFactory.get(type);
    }

    public static GeaflowConfigDesc llmDesc(GeaflowLLMType type) {
        if (type == null) {
            return null;
        }
        switch (type) {
            case OPEN_AI:
                return ConfigDescFactory.getOrRegister(OpenAIConfigArgsClass.class);
            default:
                return ConfigDescFactory.getOrRegister(LLMConfigArgsClass.class);
        }
    }

    public static GeaflowConfigDesc jobDesc(GeaflowJobType type) {
        if (type == GeaflowJobType.SERVE) {
            return ConfigDescFactory.getOrRegister(ServeJobConfigClass.class);
        }
        if (type == GeaflowJobType.INTEGRATE) {
            return ConfigDescFactory.getOrRegister(CodeJobConfigClass.class);
        }
        return ConfigDescFactory.getOrRegister(JobConfigClass.class);
    }

    /** Unknown extension values are opaque: they have no reviewed disclosure policy. */
    public static GeaflowConfig redact(GeaflowConfig config, GeaflowConfigDesc desc) {
        if (config == null) {
            return null;
        }
        Map<String, ConfigDescItem> items = new LinkedHashMap<>();
        collectItems(desc, items);
        GeaflowConfig result = new GeaflowConfig();
        config.forEach((key, value) -> {
            ConfigDescItem item = items.get(key);
            Object safe = value;
            if (value != null) {
                if (item == null || item.isMasked()) {
                    safe = MASK;
                } else if (item.getType() == GeaflowConfigType.CONFIG) {
                    safe = redactInner(value, item);
                } else if (value instanceof Map || value instanceof Iterable
                    || value.getClass().isArray()
                    || value instanceof String && EMBEDDED_CREDENTIAL.matcher((String) value).find()) {
                    safe = MASK;
                }
            }
            result.put(key, safe);
        });
        return result;
    }

    private static void collectItems(GeaflowConfigDesc desc, Map<String, ConfigDescItem> items) {
        if (desc == null) {
            return;
        }
        for (ConfigDescItem item : desc.getItems()) {
            if (item.getType() == GeaflowConfigType.CONFIG && item.getBehavior() == ConfigValueBehavior.FLATTED) {
                // A masked flattened container must not expose any of its children.
                if (!item.isMasked()) {
                    collectItems(item.getInnerConfigDesc(), items);
                }
            } else {
                items.put(item.getKey(), item);
            }
        }
    }

    private static Object redactInner(Object value, ConfigDescItem item) {
        try {
            Object nested = item.getBehavior() == ConfigValueBehavior.JSON ? JSON.parse((String) value) : value;
            if (!(nested instanceof Map)) {
                return MASK;
            }
            GeaflowConfig safe = redact(new GeaflowConfig(nested), item.getInnerConfigDesc());
            return item.getBehavior() == ConfigValueBehavior.JSON ? JSON.toJSONString(safe) : safe;
        } catch (RuntimeException ignored) {
            // Malformed opaque values must never cause fallback to the original value.
            return MASK;
        }
    }

    /** A mask means retain the old value; omission retains secrets; explicit null clears a value. */
    public static GeaflowConfig merge(GeaflowConfig update, GeaflowConfig previous, GeaflowConfigDesc desc) {
        if (update == null) {
            return previous;
        }
        Map<String, Object> old = previous == null ? new LinkedHashMap<>() : previous;
        Map<String, Object> safe = previous == null ? new LinkedHashMap<>() : redact(previous, desc);
        GeaflowConfig result = new GeaflowConfig(restore(update, old, safe));
        rejectUnresolved(result);
        return result;
    }

    private static Object restore(Object update, Object previous, Object safe) {
        if (MASK.equals(update)) {
            if (previous == null || MASK.equals(previous)) {
                throw new GeaflowIllegalException("A masked configuration value has no saved value");
            }
            return previous;
        }
        if (update instanceof Map) {
            Map<String, Object> old = previous instanceof Map ? (Map<String, Object>) previous : new LinkedHashMap<>();
            Map<String, Object> redacted = safe instanceof Map ? (Map<String, Object>) safe : new LinkedHashMap<>();
            Map<String, Object> result = new LinkedHashMap<>();
            ((Map<String, Object>) update).forEach((k, v) -> result.put(k, restore(v, old.get(k), redacted.get(k))));
            old.forEach((k, v) -> {
                if (!result.containsKey(k) && containsMask(redacted.get(k))) {
                    // Recursively retain only omitted secrets inside structured containers.
                    result.put(k, redacted.get(k) instanceof Map
                        ? restore(new LinkedHashMap<>(), v, redacted.get(k)) : v);
                }
            });
            return result;
        }
        if (update instanceof String && previous instanceof String && safe instanceof String) {
            Object updateJson = parseContainer((String) update);
            Object oldJson = parseContainer((String) previous);
            Object safeJson = parseContainer((String) safe);
            if (updateJson instanceof Map && oldJson instanceof Map && safeJson instanceof Map) {
                return JSON.toJSONString(restore(updateJson, oldJson, safeJson));
            }
        }
        return update;
    }

    public static void rejectUnresolved(Object config) {
        if (containsMask(config)) {
            throw new GeaflowIllegalException("Masked configuration values require an existing saved value");
        }
    }

    private static boolean containsMask(Object value) {
        if (MASK.equals(value)) {
            return true;
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).values().stream().anyMatch(ConfigRedactor::containsMask);
        }
        if (value instanceof List) {
            return ((List<?>) value).stream().anyMatch(ConfigRedactor::containsMask);
        }
        if (value instanceof String) {
            Object parsed = parseContainer((String) value);
            return parsed != null && containsMask(parsed);
        }
        return false;
    }

    private static Object parseContainer(String value) {
        String trimmed = value.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
            return null;
        }
        try {
            return JSON.parse(trimmed);
        } catch (RuntimeException ignored) {
            return null;
        }
    }
}
