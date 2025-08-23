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

package org.apache.geaflow.dsl.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public class CatalogFactory {

    public static Catalog getCatalog(String catalogType) {
        ServiceLoader<Catalog> graphCatalogs = ServiceLoader.load(Catalog.class);
        List<String> catalogTypes = new ArrayList<>();

        for (Catalog catalog : graphCatalogs) {
            if (catalog.getType().equals(catalogType)) {
                return catalog;
            }
            catalogTypes.add(catalog.getType());
        }
        throw new GeaFlowDSLException("Catalog type: '" + catalogType + "' is not exists, "
            + "available types are: " + catalogTypes);
    }

    public static Catalog getCatalog(Configuration conf) {
        String catalogType = conf.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TYPE);
        Catalog catalog = new CatalogImpl(getCatalog(catalogType));
        catalog.init(conf);
        return catalog;
    }
}
