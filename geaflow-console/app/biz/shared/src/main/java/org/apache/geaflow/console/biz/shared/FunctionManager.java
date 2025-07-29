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

package org.apache.geaflow.console.biz.shared;

import org.apache.geaflow.console.biz.shared.view.FunctionView;
import org.apache.geaflow.console.common.dal.model.FunctionSearch;
import org.springframework.web.multipart.MultipartFile;

public interface FunctionManager extends DataManager<FunctionView, FunctionSearch> {

    String createFunction(String instanceName, FunctionView view, MultipartFile functionFile, String fileId);

    boolean updateFunction(String instanceName, String functionName, FunctionView view, MultipartFile functionFile);

    boolean deleteFunction(String instanceName, String functionName);
}
