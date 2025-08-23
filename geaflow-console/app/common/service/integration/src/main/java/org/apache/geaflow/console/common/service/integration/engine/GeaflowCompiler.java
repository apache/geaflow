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

package org.apache.geaflow.console.common.service.integration.engine;

import java.util.Set;
import org.apache.geaflow.console.common.util.proxy.ProxyClass;

@ProxyClass("org.apache.geaflow.dsl.runtime.QueryClient")
public interface GeaflowCompiler {

    CompileResult compile(String script, CompileContext context);

    Set<FunctionInfo> getUnResolvedFunctions(String script, CompileContext context);

    Set<String> getDeclaredTablePlugins(String type, CompileContext context);

    Set<String> getEnginePlugins();

    Set<TableInfo> getUnResolvedTables(String script, CompileContext context);

    String formatOlapResult(String script, Object resultData, CompileContext context);

}
