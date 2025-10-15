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

package org.apache.geaflow.console.core.service.version;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.apache.geaflow.console.core.service.file.LocalFileFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class VersionFactory {

    private final Map<String, VersionClassLoader> classLoaderMap = new ConcurrentHashMap<>();

    @Autowired
    private LocalFileFactory localFileFactory;

    public VersionClassLoader getClassLoader(GeaflowVersion version) {
        return classLoaderMap.compute(version.getName(), (k, vcl) -> {
            if (vcl != null && GeaflowVersion.md5Equals(version, vcl.version)) {
                return vcl;
            }

            if (vcl != null) {
                vcl.closeClassLoader();
            }

            return new VersionClassLoader(version, localFileFactory);
        });
    }
}
