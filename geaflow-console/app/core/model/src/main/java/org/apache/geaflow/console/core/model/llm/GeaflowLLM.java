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

package org.apache.geaflow.console.core.model.llm;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.geaflow.console.common.util.type.GeaflowLLMType;
import org.apache.geaflow.console.core.model.GeaflowName;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowLLM extends GeaflowName {

    private String url;

    private GeaflowConfig args;

    private GeaflowLLMType type;

    public GeaflowLLM(String name, String comment) {
        super.name = name;
        super.comment = comment;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(url, "Invalid url");
        Preconditions.checkNotNull(type, "Invalid type");
        switch (type) {
            case OPEN_AI:
                args.parse(OpenAIConfigArgsClass.class);
                break;
            default:
                break;
        }

    }

}
