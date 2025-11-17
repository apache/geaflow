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
import java.lang.reflect.Method;
import org.apache.geaflow.console.core.model.llm.LocalConfigArgsClass;
import org.apache.geaflow.console.core.service.llm.LocalClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalClientTest {

    @Test
    public void testGetJsonString_WithPredictValue() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(256);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        String prompt = "Test prompt";
        String jsonString = (String) method.invoke(client, config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 256);
    }

    @Test
    public void testGetJsonString_WithNullPredict() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(null);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        String prompt = "Test prompt";
        String jsonString = (String) method.invoke(client, config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        // Should use default value 128 when predict is null
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 128);
    }

    @Test
    public void testGetJsonString_WithPromptContainingSpecialCharacters() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(100);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        // Test with special characters that need JSON escaping
        String prompt = "Test \"quoted\" prompt\nwith newline\tand tab";
        String jsonString = (String) method.invoke(client, config, prompt);
        
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
    public void testGetJsonString_WithPromptTrim() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(200);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        // Test with trailing newline that should be trimmed
        String prompt = "Test prompt\n";
        String jsonString = (String) method.invoke(client, config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 200);
    }

    @Test
    public void testGetJsonString_WithZeroPredict() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(0);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        String prompt = "Test prompt";
        String jsonString = (String) method.invoke(client, config, prompt);
        
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertEquals(json.getString("prompt"), "Test prompt");
        Assert.assertEquals(json.getInteger("n_predict").intValue(), 0);
    }

    @Test
    public void testGetJsonString_JsonStructure() throws Exception {
        LocalClient client = (LocalClient) LocalClient.getInstance();
        LocalConfigArgsClass config = new LocalConfigArgsClass();
        config.setPredict(128);
        
        // Use reflection to test private method
        Method method = LocalClient.class.getDeclaredMethod("getJsonString", 
            LocalConfigArgsClass.class, String.class);
        method.setAccessible(true);
        
        String prompt = "Test";
        String jsonString = (String) method.invoke(client, config, prompt);
        
        // Verify JSON structure
        JSONObject json = JSON.parseObject(jsonString);
        Assert.assertTrue(json.containsKey("prompt"));
        Assert.assertTrue(json.containsKey("n_predict"));
        Assert.assertEquals(json.size(), 2); // Should only have these two fields
    }
}

