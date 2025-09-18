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

package org.apache.geaflow.store.paimon;

import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_META_STORE;
import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_WAREHOUSE;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.store.paimon.config.PaimonStoreConfig;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonCatalogManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonCatalogManager.class);

    private static final Map<String, PaimonCatalogClient> catalogMap = new ConcurrentHashMap<>();

    public static synchronized PaimonCatalogClient getCatalogClient(Configuration config) {
        String warehouse = config.getString(PAIMON_STORE_WAREHOUSE);
        return catalogMap.computeIfAbsent(warehouse, k -> createCatalog(config));
    }

    private static PaimonCatalogClient createCatalog(Configuration config) {
        String metastore = config.getString(PAIMON_STORE_META_STORE);
        String warehouse = config.getString(PAIMON_STORE_WAREHOUSE);

        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouse.toLowerCase());
        options.set(CatalogOptions.METASTORE, metastore.toLowerCase());
        Map<String, String> extraOptions = PaimonStoreConfig.getPaimonOptions(config);
        if (extraOptions != null) {
            for (Map.Entry<String, String> entry : extraOptions.entrySet()) {
                LOGGER.info("add option: {}={}", entry.getKey(), entry.getValue());
                options.set(entry.getKey(), entry.getValue());
            }
        }
        if (!metastore.equalsIgnoreCase("filesystem")) {
            throw new UnsupportedOperationException("Not support meta store type " + metastore);
        }

        CatalogContext context = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(context);
        return new PaimonCatalogClient(catalog, config);
    }

}
