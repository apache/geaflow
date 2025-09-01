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

import static org.apache.geaflow.store.paimon.PaimonConfigKeys.PAIMON_OPTIONS_META_STORE;
import static org.apache.geaflow.store.paimon.PaimonConfigKeys.PAIMON_OPTIONS_WAREHOUSE;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonTableCatalogClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonTableCatalogClient.class);

    private final Configuration config;

    private Catalog catalog;

    public PaimonTableCatalogClient(Configuration config) {
        this.config = config;
    }

    public Table createTable(Schema schema, Identifier identifier) {
        this.catalog = createCatalog(config);
        try {
            this.catalog.createDatabase(identifier.getDatabaseName(), true);
            this.catalog.createTable(identifier, schema, true);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Create database or table failed.", e);
        }
        return getTable(identifier);
    }

    private Catalog createCatalog(Configuration config) {
        Options options = new Options();

        String metastore = config.getString(PAIMON_OPTIONS_META_STORE);
        String warehouse = config.getString(PAIMON_OPTIONS_WAREHOUSE);

        options.set(CatalogOptions.WAREHOUSE, warehouse.toLowerCase());
        options.set(CatalogOptions.METASTORE, metastore.toLowerCase());
        if (!metastore.equalsIgnoreCase("filesystem")) {
            throw new UnsupportedOperationException("Not support meta store type " + metastore);
        }

        if (!warehouse.startsWith("file://")) {
            throw new UnsupportedOperationException(
                "Only support warehouse path prefix: [file://]");
        }

        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    public Catalog getCatalog() {
        return this.catalog;
    }

    public Table getTable(Identifier identifier) {
        try {
            return this.catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new GeaflowRuntimeException("table not exist", e);
        }
    }

    public void close() {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                throw new GeaflowRuntimeException("Failed to close catalog.", e);
            }
        }
    }

    public void dropDatabase(String dbName) {
        try {
            catalog.dropDatabase(dbName, true, true);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to drop database.", e);
        }
    }

    public void dropTable(Identifier identifier) {
        try {
            catalog.dropTable(identifier, true);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to drop table.", e);
        }
    }
}
