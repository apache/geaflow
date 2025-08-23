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

package org.apache.geaflow.console.test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.console.common.service.integration.engine.CompileContext;
import org.apache.geaflow.console.common.service.integration.engine.CompileResult;
import org.apache.geaflow.console.common.service.integration.engine.GeaflowCompiler;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.LoaderSwitchUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.proxy.ProxyUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class GeaflowCompilerTest {

    private final static String ENGINE_JAR_PATH;

    static {
        ENGINE_JAR_PATH = "/tmp/geaflow.jar";
    }

    private static String SCRIPT;

    @BeforeClass
    public static void beforeClass() throws Exception {
        log.info(ENGINE_JAR_PATH);
        if (!FileUtil.exist(ENGINE_JAR_PATH)) {
            throw new GeaflowException("Prepare engine jar at path {}", ENGINE_JAR_PATH);
        }
        SCRIPT = IOUtils.resourceToString("/compile.sql", StandardCharsets.UTF_8);
    }

    private URLClassLoader createClassLoader() throws Exception {
        List<URL> urlList = new ArrayList<>();
        urlList.add(new File(ENGINE_JAR_PATH).toURI().toURL());
        ClassLoader extClassLoader = ClassLoader.getSystemClassLoader().getParent();
        return new URLClassLoader(urlList.toArray(new URL[]{}), extClassLoader);
    }

    @Test(enabled = false)
    public void testCompile() throws Exception {
        try (URLClassLoader classLoader = createClassLoader()) {
            LoaderSwitchUtil.run(classLoader, () -> {
                Object context = classLoader.loadClass("org.apache.geaflow.dsl.common.compile.CompileContext")
                    .newInstance();
                Object compiler = classLoader.loadClass("org.apache.geaflow.dsl.runtime.QueryClient").newInstance();

                Method method = compiler.getClass().getMethod("compile", String.class, context.getClass());
                Object result = method.invoke(compiler, SCRIPT, context);

                String physicPlan = result.getClass().getMethod("getPhysicPlan").invoke(result).toString();
                log.info(physicPlan);
            });
        }
    }

    @Test(enabled = false)
    public void testProxyCompile() throws Exception {
        try (URLClassLoader classLoader = createClassLoader()) {
            CompileContext context = ProxyUtil.newInstance(classLoader, CompileContext.class);
            GeaflowCompiler compiler = ProxyUtil.newInstance(classLoader, GeaflowCompiler.class);
            CompileResult result = compiler.compile(SCRIPT, context);
            log.info(result.getPhysicPlan().toString());
        }
    }

}
