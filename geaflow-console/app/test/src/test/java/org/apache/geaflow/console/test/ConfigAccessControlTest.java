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
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import java.util.Collections;
import org.apache.geaflow.console.biz.shared.ClusterManager;
import org.apache.geaflow.console.biz.shared.LLMManager;
import org.apache.geaflow.console.biz.shared.PluginConfigManager;
import org.apache.geaflow.console.biz.shared.PluginManager;
import org.apache.geaflow.console.biz.shared.convert.GraphViewConverter;
import org.apache.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import org.apache.geaflow.console.biz.shared.convert.TableViewConverter;
import org.apache.geaflow.console.biz.shared.impl.AuthorizationManagerImpl;
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.biz.shared.view.TableView;
import org.apache.geaflow.console.common.dal.model.ClusterSearch;
import org.apache.geaflow.console.common.dal.model.LLMSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.PluginConfigSearch;
import org.apache.geaflow.console.common.dal.model.PluginSearch;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowSecurityException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.common.util.type.GeaflowRoleType;
import org.apache.geaflow.console.core.model.config.ConfigRedactor;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.web.controller.api.ClusterController;
import org.apache.geaflow.console.web.controller.api.LLMController;
import org.apache.geaflow.console.web.controller.api.PluginConfigController;
import org.apache.geaflow.console.web.controller.api.PluginController;
import org.apache.geaflow.console.web.mvc.ConfigResponseAdvice;
import org.apache.geaflow.console.web.mvc.GeaflowWebConfig;
import org.mockito.Mockito;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigAccessControlTest {

    private ClusterController clusterController;
    private LLMController llmController;
    private PluginConfigController pluginConfigController;
    private PluginController pluginController;
    private ClusterManager clusters;
    private LLMManager llms;
    private PluginConfigManager configs;
    private PluginManager plugins;

    @BeforeMethod
    public void setUp() {
        ContextHolder.init();
        ContextHolder.get().setUserId("tenant-user");
        ContextHolder.get().setTenantId("tenant-a");
        clusterController = new ClusterController();
        llmController = new LLMController();
        pluginConfigController = new PluginConfigController();
        pluginController = new PluginController();
        clusters = Mockito.mock(ClusterManager.class);
        llms = Mockito.mock(LLMManager.class);
        configs = Mockito.mock(PluginConfigManager.class);
        plugins = Mockito.mock(PluginManager.class);
        ReflectionTestUtils.setField(clusterController, "clusterManager", clusters);
        ReflectionTestUtils.setField(llmController, "llmManager", llms);
        ReflectionTestUtils.setField(pluginConfigController, "pluginConfigManager", configs);
        ReflectionTestUtils.setField(pluginController, "pluginManager", plugins);
        for (Object controller : new Object[]{clusterController, llmController, pluginConfigController, pluginController}) {
            ReflectionTestUtils.setField(controller, "authorizationManager", new AuthorizationManagerImpl());
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        ContextHolder.destroy();
    }

    @Test
    public void ordinaryUsersAndTenantAdminsCannotReadManagementConfigs() {
        for (String tenant : new String[]{"tenant-a", "tenant-b"}) {
            ContextHolder.get().setTenantId(tenant);
            for (boolean admin : new boolean[]{false, true}) {
                ContextHolder.get().getRoleTypes().clear();
                if (admin) {
                    ContextHolder.get().getRoleTypes().add(GeaflowRoleType.TENANT_ADMIN);
                }
                Assert.expectThrows(GeaflowSecurityException.class,
                    () -> clusterController.searchClusters(new ClusterSearch()));
                Assert.expectThrows(GeaflowSecurityException.class, () -> clusterController.queryCluster("shared"));
                Assert.expectThrows(GeaflowSecurityException.class, () -> llmController.searchLLMs(new LLMSearch()));
                Assert.expectThrows(GeaflowSecurityException.class, () -> llmController.queryLLM("shared"));
            }
        }
        Mockito.verifyNoInteractions(clusters, llms);
    }

    @Test
    public void systemSessionWithoutTheRoleCannotReadOrWritePlugins() {
        ContextHolder.get().setSystemSession(true);
        Assert.expectThrows(GeaflowSecurityException.class,
            () -> pluginConfigController.searchPluginConfigs(new PluginConfigSearch()));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginConfigController.getPluginConfig("id"));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginConfigController.createPluginConfig(new PluginConfigView()));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginConfigController.updatePluginConfig("id", new PluginConfigView()));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginConfigController.deletePluginConfig("id"));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginController.searchPlugins(new PluginSearch()));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginController.queryPlugin("id"));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginController.createPlugin(null, null, null));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginController.updatePlugin("id", null, null));
        Assert.expectThrows(GeaflowSecurityException.class, () -> pluginController.deletePlugin("id"));
        Mockito.verifyNoInteractions(configs, plugins);
    }

    @Test
    public void tenantSelectorsExposeOnlyIdNameAndType() throws Exception {
        ClusterView cluster = cluster();
        Mockito.when(clusters.search(Mockito.any())).thenReturn(new PageList<>(Collections.singletonList(cluster)));
        MockMvc mvc = MockMvcBuilders.standaloneSetup(clusterController)
            .setControllerAdvice(new ConfigResponseAdvice()).build();
        MvcResult result = mvc.perform(MockMvcRequestBuilders.get("/clusters/-/options")).andReturn();
        Assert.assertEquals(result.getResponse().getStatus(), 200);
        JSONObject option = JSON.parseObject(result.getResponse().getContentAsString())
            .getJSONObject("data").getJSONArray("list").getJSONObject(0);
        Assert.assertEquals(option.size(), 3);
        Assert.assertEquals(option.getString("name"), "shared");
        Assert.assertFalse(result.getResponse().getContentAsString().contains("synthetic-secret"));
    }

    @Test
    public void adminWireResponsesAreSafeWithBothHttpConverters() throws Exception {
        ContextHolder.get().getRoleTypes().add(GeaflowRoleType.SYSTEM_ADMIN);
        ClusterView cluster = cluster();
        Mockito.when(clusters.getByName("shared")).thenReturn(cluster);
        HttpMessageConverter<?> fastjson = new GeaflowWebConfig().fastJsonConfigure().getConverters().stream()
            .filter(converter -> converter instanceof FastJsonHttpMessageConverter).findFirst().get();
        for (HttpMessageConverter<?> converter : new HttpMessageConverter<?>[]{
            new MappingJackson2HttpMessageConverter(), fastjson}) {
            MockMvc mvc = MockMvcBuilders.standaloneSetup(clusterController)
                .setControllerAdvice(new ConfigResponseAdvice()).setMessageConverters(converter).build();
            MvcResult result = mvc.perform(MockMvcRequestBuilders.get("/clusters/shared")).andReturn();
            Assert.assertEquals(result.getResponse().getStatus(), 200);
            String body = result.getResponse().getContentAsString();
            Assert.assertFalse(body.contains("synthetic-secret"));
            Assert.assertTrue(body.contains(ConfigRedactor.MASK));
            Assert.assertEquals(result.getResponse().getHeader("Cache-Control"), "no-store");
            Assert.assertEquals(cluster.getConfig().get("kubernetes.cert.key"), "synthetic-secret");
        }
    }

    @Test
    public void tableAndGraphEditsMergeTheSavedPluginConfiguration() {
        PluginConfigViewConverter converter = new PluginConfigViewConverter();
        TableViewConverter tables = new TableViewConverter();
        GraphViewConverter graphs = new GraphViewConverter();
        ReflectionTestUtils.setField(tables, "pluginConfigViewConverter", converter);
        ReflectionTestUtils.setField(graphs, "pluginConfigViewConverter", converter);
        TableView table = new TableView();
        table.setPluginConfig(plugin("synthetic-secret"));
        TableView tableEdit = new TableView();
        tableEdit.setPluginConfig(plugin(ConfigRedactor.MASK));
        tables.merge(table, tableEdit);
        Assert.assertEquals(table.getPluginConfig().getConfig().get("geaflow.store.redis.password"), "synthetic-secret");
        GraphView graph = new GraphView();
        graph.setPluginConfig(plugin("synthetic-secret"));
        graph.getPluginConfig().setCategory(GeaflowPluginCategory.GRAPH);
        GraphView graphEdit = new GraphView();
        graphEdit.setPluginConfig(plugin(ConfigRedactor.MASK));
        graphs.merge(graph, graphEdit);
        Assert.assertEquals(graph.getPluginConfig().getConfig().get("geaflow.store.redis.password"), "synthetic-secret");
    }

    private static ClusterView cluster() {
        ClusterView view = new ClusterView();
        view.setId("cluster-id");
        view.setName("shared");
        view.setType(GeaflowPluginType.K8S);
        GeaflowConfig config = new GeaflowConfig();
        config.put("kubernetes.cert.key", "synthetic-secret");
        view.setConfig(config);
        return view;
    }

    private static PluginConfigView plugin(String password) {
        PluginConfigView view = new PluginConfigView();
        view.setId("config-id");
        view.setType(GeaflowPluginType.REDIS.name());
        view.setCategory(GeaflowPluginCategory.TABLE);
        GeaflowConfig config = new GeaflowConfig();
        config.put("geaflow.store.redis.password", password);
        view.setConfig(config);
        return view;
    }
}
