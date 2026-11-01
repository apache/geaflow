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
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.biz.shared.convert.ClusterViewConverter;
import org.apache.geaflow.console.biz.shared.convert.LLMViewConverter;
import org.apache.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import org.apache.geaflow.console.biz.shared.convert.ReleaseUpdateViewConverter;
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.biz.shared.view.LLMView;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.biz.shared.view.ReleaseUpdateView;
import org.apache.geaflow.console.biz.shared.view.ReleaseView;
import org.apache.geaflow.console.biz.shared.view.TableView;
import org.apache.geaflow.console.biz.shared.view.TaskView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowLLMType;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.config.ConfigDescFactory;
import org.apache.geaflow.console.core.model.config.ConfigRedactor;
import org.apache.geaflow.console.core.model.config.ConfigValueBehavior;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.config.GeaflowConfigClass;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;
import org.apache.geaflow.console.core.model.job.GeaflowCustomJob;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.ReleaseUpdate;
import org.apache.geaflow.console.web.api.ConfigViewFilter;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.mock.web.MockHttpServletResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigSecurityTest {

    private static final String SECRET = "synthetic-secret-do-not-disclose";

    private static GeaflowConfig config(String key, Object value) {
        GeaflowConfig config = new GeaflowConfig();
        config.put(key, value);
        return config;
    }

    private static PluginConfigView plugin(GeaflowPluginType type, GeaflowConfig config) {
        PluginConfigView view = new PluginConfigView();
        view.setType(type.name());
        view.setCategory(GeaflowPluginCategory.TABLE);
        view.setConfig(config);
        return view;
    }

    @Test
    public void redactsEveryCredentialFamilyWithoutChangingSource() {
        String[][] cases = {
            {"JDBC", "geaflow.store.jdbc.password"},
            {"JDBC", "geaflow.store.jdbc.connect.config.json"},
            {"REDIS", "geaflow.store.redis.password"},
            {"INFLUXDB", "geaflow.metric.influxdb.token"},
            {"OSS", "geaflow.file.oss.secret.key"},
            {"K8S", "kubernetes.cert.data"},
            {"K8S", "kubernetes.cert.key"}
        };
        for (String[] item : cases) {
            GeaflowPluginType type = GeaflowPluginType.valueOf(item[0]);
            GeaflowConfig source = config(item[1], SECRET);
            source.put("custom.extension", config("opaque", SECRET));
            GeaflowConfig safe = ConfigRedactor.redact(source, ConfigRedactor.pluginDesc(type));
            Assert.assertEquals(safe.get(item[1]), ConfigRedactor.MASK);
            Assert.assertEquals(safe.get("custom.extension"), ConfigRedactor.MASK);
            Assert.assertEquals(source.get(item[1]), SECRET);
            Assert.assertNotSame(safe, source);
        }
    }

    @Test
    public void unknownTypesAndEmbeddedCredentialsFailClosed() {
        PluginConfigView custom = plugin(GeaflowPluginType.None, config("unrecognizable", SECRET));
        custom.setType("CUSTOM_PLUGIN");
        String json = JSON.toJSONString(custom, new ConfigViewFilter());
        Assert.assertFalse(json.contains(SECRET));
        for (String url : Arrays.asList("jdbc:mysql://host/db?password=" + SECRET,
            "https://user:" + SECRET + "@host/db")) {
            GeaflowConfig safe = ConfigRedactor.redact(config("geaflow.store.jdbc.url", url),
                ConfigRedactor.pluginDesc(GeaflowPluginType.JDBC));
            Assert.assertEquals(safe.get("geaflow.store.jdbc.url"), ConfigRedactor.MASK);
        }
        GeaflowConfig safe = ConfigRedactor.redact(config("geaflow.store.redis.port", 6379),
            ConfigRedactor.pluginDesc(GeaflowPluginType.REDIS));
        Assert.assertEquals(safe.get("geaflow.store.redis.port"), 6379);
    }

    @Test
    public void coversPagesAndNestedTasksGraphsTablesAndLlmStrings() throws Exception {
        PluginConfigView plugin = plugin(GeaflowPluginType.JDBC, config("geaflow.store.jdbc.password", SECRET));
        TableView table = new TableView();
        table.setPluginConfig(plugin);
        GraphView graph = new GraphView();
        graph.setPluginConfig(plugin);
        TaskView task = new TaskView();
        task.setRuntimeMetaPluginConfig(plugin);
        task.setHaMetaPluginConfig(plugin);
        ReleaseView release = new ReleaseView();
        release.setClusterConfig(config("hidden-extension", SECRET));
        task.setRelease(release);
        ClusterView cluster = new ClusterView();
        cluster.setType(GeaflowPluginType.K8S);
        cluster.setConfig(config("kubernetes.cert.key", SECRET));
        LLMView llm = new LLMView();
        llm.setType(GeaflowLLMType.OPEN_AI);
        llm.setArgs(JSON.toJSONString(config("apiKey", SECRET)));
        GeaflowApiResponse<?> response = GeaflowApiResponse.success(
            new PageList<>(Arrays.asList(table, graph, task, cluster, llm)));
        String wire = JSON.toJSONString(response, new ConfigViewFilter());
        Assert.assertFalse(wire.contains(SECRET));
        Assert.assertTrue(wire.contains(ConfigRedactor.MASK));
        Assert.assertEquals(plugin.getConfig().get("geaflow.store.jdbc.password"), SECRET);
        Assert.assertTrue(llm.getArgs().contains(SECRET));
        MockHttpServletResponse servletResponse = new MockHttpServletResponse();
        response.write(servletResponse);
        Assert.assertFalse(servletResponse.getContentAsString().contains(SECRET));
        Assert.assertEquals(servletResponse.getHeader("Cache-Control"), "no-store");
        JSONObject llmJson = JSON.parseObject(JSON.toJSONString(llm, new ConfigViewFilter()));
        Assert.assertEquals(JSON.parseObject(llmJson.getString("args")).getString("apiKey"), ConfigRedactor.MASK);
    }

    @Test
    public void masksNestedJsonAndFlattenedSchemasAndRestoresUpdates() {
        GeaflowConfig original = config("nested", config("secret", SECRET));
        original.put("encoded", JSON.toJSONString(config("secret", SECRET)));
        original.put("flatSecret", SECRET);
        GeaflowConfig safe = ConfigRedactor.redact(original, ConfigDescFactory.getOrRegister(Nested.class));
        Assert.assertFalse(JSON.toJSONString(safe).contains(SECRET));
        Assert.assertEquals(ConfigRedactor.merge(safe, original, ConfigDescFactory.getOrRegister(Nested.class)), original);
        GeaflowConfig malformed = config("encoded", "invalid-json-" + SECRET);
        Assert.assertFalse(JSON.toJSONString(ConfigRedactor.redact(malformed,
            ConfigDescFactory.getOrRegister(Nested.class))).contains(SECRET));
    }

    @Test
    public void editMasksAndOmittedSecretsPreserveSavedValuesAndNullClears() {
        PluginConfigView saved = plugin(GeaflowPluginType.REDIS, config("geaflow.store.redis.password", SECRET));
        saved.getConfig().put("geaflow.store.redis.port", 6379);
        PluginConfigView edit = plugin(GeaflowPluginType.REDIS, config("geaflow.store.redis.password", ConfigRedactor.MASK));
        PluginConfigViewConverter converter = new PluginConfigViewConverter();
        converter.merge(saved, edit);
        Assert.assertEquals(converter.convert(saved).getConfig().get("geaflow.store.redis.password"), SECRET);
        edit.setConfig(config("geaflow.store.redis.port", 6380));
        converter.merge(saved, edit);
        Assert.assertEquals(saved.getConfig().get("geaflow.store.redis.password"), SECRET);
        Assert.assertEquals(saved.getConfig().get("geaflow.store.redis.port"), 6380);
        edit.setConfig(config("geaflow.store.redis.password", null));
        converter.merge(saved, edit);
        Assert.assertTrue(saved.getConfig().containsKey("geaflow.store.redis.password"));
        Assert.assertNull(saved.getConfig().get("geaflow.store.redis.password"));
        edit.setConfig(config("geaflow.store.redis.password", "rotated-secret"));
        converter.merge(saved, edit);
        Assert.assertEquals(converter.convert(saved).getConfig().get("geaflow.store.redis.password"), "rotated-secret");
    }

    @Test(expectedExceptions = GeaflowIllegalException.class)
    public void createRejectsUnresolvedMasks() {
        new PluginConfigViewConverter().convert(plugin(GeaflowPluginType.REDIS,
            config("geaflow.store.redis.password", ConfigRedactor.MASK)));
    }

    @Test(expectedExceptions = GeaflowIllegalException.class)
    public void typeChangeCannotReuseAnOldMaskedCredential() {
        PluginConfigView saved = plugin(GeaflowPluginType.REDIS, config("geaflow.store.redis.password", SECRET));
        PluginConfigView edit = plugin(GeaflowPluginType.JDBC, config("geaflow.store.redis.password", ConfigRedactor.MASK));
        new PluginConfigViewConverter().merge(saved, edit);
    }

    @Test(expectedExceptions = GeaflowIllegalException.class)
    public void llmUrlChangeCannotReuseAnOldMaskedKey() {
        LLMView saved = new LLMView();
        saved.setType(GeaflowLLMType.OPEN_AI);
        saved.setUrl("https://original.example/api");
        saved.setArgs(JSON.toJSONString(config("apiKey", SECRET)));
        LLMView edit = new LLMView();
        edit.setUrl("https://different.example/api");
        edit.setArgs(JSON.toJSONString(config("apiKey", ConfigRedactor.MASK)));
        new LLMViewConverter().merge(saved, edit);
    }

    @Test
    public void clusterAndLlmEditsPreserveSecrets() {
        ClusterView cluster = new ClusterView();
        cluster.setType(GeaflowPluginType.K8S);
        cluster.setConfig(config("kubernetes.cert.key", SECRET));
        ClusterView edit = new ClusterView();
        edit.setConfig(config("kubernetes.cert.key", ConfigRedactor.MASK));
        new ClusterViewConverter().merge(cluster, edit);
        Assert.assertEquals(new ClusterViewConverter().convert(cluster).getConfig().get("kubernetes.cert.key"), SECRET);
        LLMView llm = new LLMView();
        llm.setType(GeaflowLLMType.OPEN_AI);
        llm.setArgs(JSON.toJSONString(config("apiKey", SECRET)));
        LLMView llmEdit = new LLMView();
        llmEdit.setArgs(JSON.toJSONString(config("apiKey", ConfigRedactor.MASK)));
        new LLMViewConverter().merge(llm, llmEdit);
        Assert.assertEquals(new LLMViewConverter().convert(llm).getArgs().get("apiKey"), SECRET);
    }

    @Test
    public void releaseEditsRestoreSecretsWhenClusterSelectionIsOmitted() {
        GeaflowRelease release = new GeaflowRelease();
        release.setJob(new GeaflowCustomJob());
        release.setJobConfig(config("extension", SECRET));
        release.setClusterConfig(config("extension", SECRET));
        ReleaseUpdateView edit = new ReleaseUpdateView();
        edit.setNewJobConfig(config("extension", ConfigRedactor.MASK));
        edit.setNewClusterConfig(config("extension", ConfigRedactor.MASK));
        ReleaseUpdate restored = new ReleaseUpdateViewConverter().converter(edit, null, null, release);
        Assert.assertEquals(restored.getNewJobConfig().get("extension"), SECRET);
        Assert.assertEquals(restored.getNewClusterConfig().get("extension"), SECRET);
        Assert.assertEquals(edit.getNewClusterConfig().get("extension"), ConfigRedactor.MASK);
        ReleaseUpdate noChange = new ReleaseUpdateViewConverter().converter(new ReleaseUpdateView(), null, null, release);
        Assert.assertNull(noChange.getNewJobConfig());
        Assert.assertNull(noChange.getNewClusterConfig());
        GeaflowCluster differentCluster = new GeaflowCluster();
        differentCluster.setId("different-cluster");
        Assert.expectThrows(GeaflowIllegalException.class,
            () -> new ReleaseUpdateViewConverter().converter(edit, null, differentCluster, release));
    }

    @Getter
    @Setter
    public static class Secrets extends GeaflowConfigClass {

        @GeaflowConfigKey("secret")
        @GeaflowConfigValue(masked = true)
        private String secret;
    }

    @Getter
    @Setter
    public static class FlatSecrets extends GeaflowConfigClass {

        @GeaflowConfigKey("flatSecret")
        @GeaflowConfigValue(masked = true)
        private String secret;
    }

    @Getter
    @Setter
    public static class Nested extends GeaflowConfigClass {

        @GeaflowConfigKey("nested")
        private Secrets nested;

        @GeaflowConfigKey("encoded")
        @GeaflowConfigValue(behavior = ConfigValueBehavior.JSON)
        private Secrets encoded;

        @GeaflowConfigKey("flattened")
        @GeaflowConfigValue(behavior = ConfigValueBehavior.FLATTED)
        private FlatSecrets flattened;
    }
}
