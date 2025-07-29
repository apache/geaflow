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

package org.apache.geaflow.console.biz.shared.view;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;

@Setter
@Getter
public class JobView extends NameView {

    private String instanceId;

    private String instanceName;

    private GeaflowJobType type;

    private String userCode;

    private String structMappings;

    private List<StructView> structs;

    private List<GraphView> graphs;

    private SlaView sla;

    private List<FunctionView> functions;

    private String entryClass;

    private RemoteFileView jarPackage;
}
