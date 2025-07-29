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

package org.apache.geaflow.console.core.service.converter;

import java.util.Optional;
import org.apache.geaflow.console.common.dal.entity.VersionEntity;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.springframework.stereotype.Component;

@Component
public class VersionConverter extends NameConverter<GeaflowVersion, VersionEntity> {

    @Override
    protected VersionEntity modelToEntity(GeaflowVersion model) {
        VersionEntity version = super.modelToEntity(model);
        version.setPublish(model.isPublish());
        Optional.ofNullable(model.getEngineJarPackage()).map(GeaflowId::getId).ifPresent(version::setEngineJarId);
        Optional.ofNullable(model.getLangJarPackage()).map(GeaflowId::getId).ifPresent(version::setLangJarId);
        return version;
    }


    @Override
    protected GeaflowVersion entityToModel(VersionEntity entity) {
        GeaflowVersion geaflowVersion = super.entityToModel(entity);
        geaflowVersion.setPublish(entity.isPublish());
        return geaflowVersion;
    }

    public GeaflowVersion convert(VersionEntity entity, GeaflowRemoteFile engineJar, GeaflowRemoteFile langJar) {
        GeaflowVersion version = entityToModel(entity);
        version.setEngineJarPackage(engineJar);
        version.setLangJarPackage(langJar);
        return version;
    }
}
