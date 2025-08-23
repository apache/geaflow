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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.proxy.ProxyUtil;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;

@Slf4j
public class FunctionClassLoader implements CompileClassLoader {

    private final URLClassLoader functionLoader;

    private final VersionClassLoader versionClassLoader;

    public FunctionClassLoader(VersionClassLoader versionClassLoader, List<GeaflowRemoteFile> jars) {
        this.versionClassLoader = versionClassLoader;
        this.functionLoader = createFunctionLoader(jars);
    }

    public FunctionClassLoader(VersionClassLoader versionClassLoader, URL[] urls) {
        this.versionClassLoader = versionClassLoader;
        this.functionLoader = createFunctionLoader(urls);
    }

    private URLClassLoader createFunctionLoader(URL[] urls) {
        return new URLClassLoader(urls, versionClassLoader.getClassLoader());
    }

    private URLClassLoader createFunctionLoader(List<GeaflowRemoteFile> userJars) {
        List<URL> userUrls = ListUtil.convert(userJars, jar -> {
            try {
                File file = versionClassLoader.getLocalFileFactory().getUserFile(jar.getCreatorId(), jar);
                return file.toURI().toURL();

            } catch (Exception e) {
                throw new GeaflowException("Add function jar file {} failed", jar.getName(), e);
            }
        });

        return new URLClassLoader(userUrls.toArray(new URL[]{}), versionClassLoader.getClassLoader());

    }

    @Override
    public <T> T newInstance(Class<T> clazz, Object... parameters) {
        return ProxyUtil.newInstance(functionLoader, clazz, parameters);
    }

    public void closeClassLoader() {
        String files = Arrays.stream(functionLoader.getURLs()).map(URL::getFile).collect(Collectors.joining(";"));
        try {
            functionLoader.close();
            log.info("Close functionLoader {}", files);

        } catch (Exception e) {
            log.info("Fail to close functionLoader {}", files);
        }
    }
}
