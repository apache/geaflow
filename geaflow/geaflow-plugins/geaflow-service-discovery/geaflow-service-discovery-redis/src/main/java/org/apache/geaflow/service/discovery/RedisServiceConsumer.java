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

package org.apache.geaflow.service.discovery;

import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisServiceConsumer implements ServiceConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisServiceConsumer.class);

    private final RecoverableRedis recoverableRedis;

    private final String baseKey;

    private final String namespace;

    public RedisServiceConsumer(Configuration configuration) {
        this.recoverableRedis = new RecoverableRedis();
        String appName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.baseKey = appName.startsWith("/") ? appName : "/" + appName;
        StoreContext storeContext = new StoreContext(baseKey);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, null));
        storeContext.withConfig(configuration);
        this.recoverableRedis.init(storeContext);
        this.namespace = recoverableRedis.getNamespace();
        LOGGER.info("redis service consumer base key is {}, namespace is {}", this.baseKey, this.namespace);
    }

    @Override
    public boolean exists(String path) {
        if (StringUtils.isBlank(path)) {
            return this.recoverableRedis.getData(this.namespace) != null;
        }
        return this.recoverableRedis.getData(path) != null;
    }

    @Override
    public byte[] getDataAndWatch(String path) {
        return this.recoverableRedis.getData(path);
    }


    @Override
    public void close() {
        if (this.recoverableRedis != null) {
            this.recoverableRedis.close();
        }
    }

}
