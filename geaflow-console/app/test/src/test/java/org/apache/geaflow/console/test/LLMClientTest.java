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

import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.llm.CodefuseConfigArgsClass;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.apache.geaflow.console.core.model.llm.OpenAIConfigArgsClass;
import org.apache.geaflow.console.core.service.llm.CodefuseClient;
import org.apache.geaflow.console.core.service.llm.LLMClient;
import org.apache.geaflow.console.core.service.llm.LocalClient;
import org.apache.geaflow.console.core.service.llm.OpenAiClient;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class LLMClientTest {

    @Test(enabled = false)
    public void testLocal() {
        LLMClient llmClient = LocalClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("http://127.0.0.1:8000/completion");
        String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
        log.info(answer);
    }

    @Test(enabled = false)
    public void testOpenAI() {

        LLMClient llmClient = OpenAiClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("https://api.openai.com/v1/chat/completions");

        GeaflowConfig geaflowConfig = new GeaflowConfig();

        OpenAIConfigArgsClass configArgsClass = new OpenAIConfigArgsClass();
        // set own sk
        String sk = "sk-xxxx";
        configArgsClass.setApiKey(sk);

        Assert.assertThrows(GeaflowException.class, () -> geaflowConfig.parse(OpenAIConfigArgsClass.class));

        configArgsClass.setModelId("ft:gpt-3.5-turbo-1106:personal:geaflow:8zLbe4Ua");
        geaflowLLM.setArgs(configArgsClass.build());

        for (int i = 0; i < 5; i++) {
            String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
            log.info(answer);
        }


    }

    @Test(enabled = false)
    public void testCodefuse() {
        LLMClient llmClient = CodefuseClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("https://riskautopilot-pre.alipay.com/v1/gpt/codegpt/task");

        CodefuseConfigArgsClass configArgsClass = new CodefuseConfigArgsClass();
        configArgsClass.setChainName("v18");
        configArgsClass.setSceneName("codegpt_single_finetune_v18");
        geaflowLLM.setArgs(configArgsClass.build());

        String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
        log.info(answer);
    }


}
