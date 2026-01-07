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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.geaflow.console.core.model.llm.LocalConfigArgsClass;
import org.apache.geaflow.console.core.service.llm.LocalClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalClientTest {

    @Test
    public void testGetJsonString_WithPredictValue() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(256);
        
        String prompt = "Test prompt";
        String jsonString = client.getJsonString(config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 256);
    }

    @Test
    public void testGetJsonString_WithNullPredict() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(null);
        
        String prompt = "Test prompt";
        String jsonString = client.getJsonString(config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        // Should use default value 128 when predict is null
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 128);
    }

    @Test
    public void testGetJsonString_WithPromptContainingSpecialCharacters() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(100);
        
        // Test with special characters that need JSON escaping
        String prompt = "Test \"quoted\" prompt\nwith newline\tand tab";
        String jsonString = client.getJsonString(config, prompt);
        
        // Should be valid JSON and properly escaped
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertNotNull(json);
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 100);
        
        // The prompt should be properly escaped in JSON string (check raw JSON string)
        // In the raw JSON string, quotes should be escaped as \"
        Assert.assertTrue(jsonString.contains("\\\""), 
            "JSON string should contain escaped quotes");
        
        // Verify the parsed prompt matches the original (special characters preserved)
        String parsedPrompt = json.getString("prompt");
        Assert.assertNotNull(parsedPrompt);
        Assert.assertEquals(parsedPrompt, prompt, 
            "Parsed prompt should match original prompt with special characters");
    }

    @Test
    public void testGetJsonString_WithPromptTrim() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(200);
        
        // Test with trailing newline that should be trimmed
        String prompt = "Test prompt\n";
        String jsonString = client.getJsonString(config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 200);
    }

    @Test
    public void testGetJsonString_WithZeroPredict() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(0);
        
        String prompt = "Test prompt";
        String jsonString = client.getJsonString(config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 0);
    }

    @Test
    public void testGetJsonString_JsonStructure() {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(128);
        
        String prompt = "Test";
        String jsonString = client.getJsonString(config, prompt);
        
        // Verify JSON structure
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertTrue(json.containsKey("prompt"));
        Assert.assertTrue(json.containsKey("n_predict"));
        Assert.assertEquals(json.size(), 2); // Should only have these two fields
    }
}

