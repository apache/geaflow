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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonCatalogClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonCatalogClient.class);

    private final Configuration config;

    private Catalog catalog;

    public PaimonCatalogClient(Catalog catalog, Configuration config) {
        this.config = config;
        this.catalog = catalog;
    }

    public Table createTable(Schema schema, Identifier identifier) {
        try {
            LOGGER.info("create table {}", identifier.getFullName());
            this.catalog.createDatabase(identifier.getDatabaseName(), true);
            this.catalog.createTable(identifier, schema, true);
            return getTable(identifier);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Create database or table failed.", e);
        }
    }

    public Catalog getCatalog() {
        return this.catalog;
    }

    public Table getTable(Identifier identifier) throws TableNotExistException {
        return this.catalog.getTable(identifier);
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
