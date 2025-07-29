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

package org.apache.geaflow.console.core.service.store.impl;

import static org.apache.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass.MYSQL_DRIVER_CLASS;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.DateTimeUtil;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.job.config.RuntimeMetaArgsClass;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricMeta;
import org.apache.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import org.apache.geaflow.console.core.model.runtime.GeaflowCycle;
import org.apache.geaflow.console.core.model.runtime.GeaflowError;
import org.apache.geaflow.console.core.model.runtime.GeaflowOffset;
import org.apache.geaflow.console.core.model.runtime.GeaflowPipeline;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.runtime.TaskParams;
import org.apache.geaflow.console.core.service.store.GeaflowRuntimeMetaStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

@Component
public class JdbcStore implements GeaflowRuntimeMetaStore {

    private static final String QUERY_TASK_ALL_KEY = "_";

    private static final String QUERY_TASK_OFFSET_KEY = "_offset_";

    private static final String QUERY_TASK_EXCEPTION_KEY = "_exception_";

    private static final String QUERY_METRIC_META_KEY = "_metrics_META_";

    private static final String QUERY_HEARTBEAT_KEY = "_heartbeat_";

    private static final String QUERY_TASK_PIPELINE_KEY = "_metrics_PIPELINE_";

    private static final String QUERY_TASK_CYCLE_KEY = "_metrics_CYCLE_";


    private static final Object lock = new Object();
    private static final Map<String, JdbcTemplate> jdbcTemplateCache = new ConcurrentHashMap();

    private static String getSqlByPkLimitedAndOrdered(String tableName, String pk, Integer count) {
        return String.format(
            "SELECT pk, value, gmt_modified from %s WHERE pk LIKE '%s%%' order by gmt_modified desc limit %s",
            tableName, pk, count);
    }

    private static String getSqlByPkLimitedAndOrderedWithStartTime(String tableName, String pk, Integer count,
                                                                   Long startTime) {
        if (startTime == null) {
            return getSqlByPkLimitedAndOrdered(tableName, pk, count);
        }
        return String.format("SELECT pk, value, gmt_modified from %s WHERE pk LIKE '%s%%' AND gmt_modified > '%s' "
            + "order by gmt_modified desc limit %s", tableName, pk, DateTimeUtil.format(new Date(startTime)), count);
    }

    private static String deleteSqlByPkLike(String tableName, String pk) {
        return String.format("DELETE from %s WHERE pk LIKE '%s%%'", tableName, pk);
    }

    @Override
    public PageList<GeaflowPipeline> queryPipelines(GeaflowTask task) {
        List<RuntimeMeta> pipelines = queryMessage(task, getQueryKey(task, QUERY_TASK_PIPELINE_KEY), 500, null);
        return new PageList<>(ListUtil.convert(pipelines, pipeline -> JSON.parseObject(pipeline.getValue(), GeaflowPipeline.class)));
    }

    @Override
    public PageList<GeaflowCycle> queryCycles(GeaflowTask task, String pipelineId) {
        List<RuntimeMeta> cycles = queryMessage(task, getQueryKey(task, QUERY_TASK_CYCLE_KEY + pipelineId), 500, null);
        return new PageList<>(ListUtil.convert(cycles, cycle -> JSON.parseObject(cycle.getValue(), GeaflowCycle.class)));
    }

    @Override
    public PageList<GeaflowOffset> queryOffsets(GeaflowTask task) {
        String queryKey = getQueryKey(task, QUERY_TASK_OFFSET_KEY);
        List<RuntimeMeta> offsets = queryMessage(task, queryKey, 500, null);
        List<GeaflowOffset> list = ListUtil.convert(offsets, e -> {
            GeaflowOffset offset = JSON.parseObject(e.getValue(), GeaflowOffset.class);
            offset.setPartitionName(e.getPk().substring(queryKey.length()));
            offset.formatTime();
            return offset;
        });
        return new PageList<>(list);
    }

    @Override
    public PageList<GeaflowError> queryErrors(GeaflowTask task) {
        List<RuntimeMeta> errors = queryMessage(task, getQueryKey(task, QUERY_TASK_EXCEPTION_KEY), 500, 0L);
        return new PageList<>(ListUtil.convert(errors, error -> JSON.parseObject(error.getValue(), GeaflowError.class)));
    }

    @Override
    public PageList<GeaflowMetricMeta> queryMetricMeta(GeaflowTask task) {
        List<RuntimeMeta> metricMetaList = queryMessage(task, getQueryKey(task, QUERY_METRIC_META_KEY), 500, 0L);
        return new PageList<>(ListUtil.convert(metricMetaList, metricMeta -> {
            GeaflowMetricMeta meta = JSON.parseObject(metricMeta.getValue(), GeaflowMetricMeta.class);
            String fullName = meta.getMetricName();
            if (fullName.contains("/")) {
                String groupName = StringUtils.substringBefore(fullName, "/");
                String metricName = StringUtils.substringAfter(fullName, "/");
                meta.setMetricGroup(groupName);
                meta.setMetricName(metricName);
            }
            return meta;
        }));
    }

    @Override
    public GeaflowHeartbeatInfo queryHeartbeat(GeaflowTask task) {
        List<RuntimeMeta> heartbeatList = queryMessage(task, getQueryKey(task, QUERY_HEARTBEAT_KEY), 1, 0L);
        if (CollectionUtils.isEmpty(heartbeatList)) {
            return null;
        }
        return JSON.parseObject(heartbeatList.get(0).getValue(), GeaflowHeartbeatInfo.class);
    }

    @Override
    public void cleanRuntimeMeta(GeaflowTask task) {
        RuntimeMetaArgsClass runtimeMetaArgs = new RuntimeMetaArgsClass(task.getRuntimeMetaPluginConfig());
        JdbcTemplate template = buildJdbcTemplate(((JdbcPluginConfigClass) runtimeMetaArgs.getPlugin()));
        String tableName = runtimeMetaArgs.getTable();
        String key = getQueryKey(task, QUERY_TASK_ALL_KEY);
        String sql = deleteSqlByPkLike(tableName, key);
        template.execute(sql);
    }

    private String getQueryKey(GeaflowTask task, String separator) {
        return TaskParams.getRuntimeTaskName(task.getId()) + separator;
    }

    private List<RuntimeMeta> queryMessage(GeaflowTask task, String queryKey, int maxCount, Long startTime) {
        RuntimeMetaArgsClass runtimeMetaArgs = new RuntimeMetaArgsClass(task.getRuntimeMetaPluginConfig());

        List<RuntimeMeta> result = Lists.newArrayListWithCapacity(maxCount);
        String tableName = runtimeMetaArgs.getTable();
        String sql = getSqlByPkLimitedAndOrderedWithStartTime(tableName, queryKey, maxCount, startTime);
        try {
            JdbcTemplate jdbcTemplate = buildJdbcTemplate((JdbcPluginConfigClass) runtimeMetaArgs.getPlugin());
            if (!checkTableExist(jdbcTemplate, tableName)) {
                return null;
            }
            List<Map<String, Object>> resultList = jdbcTemplate.queryForList(sql);
            Iterator<Map<String, Object>> iterator = resultList.iterator();
            int count = 0;
            while (iterator.hasNext() && count < maxCount) {
                Map<String, Object> rs = iterator.next();
                String pk = rs.get("pk").toString();
                String value = new String((byte[]) rs.get("value"), StandardCharsets.UTF_8);
                result.add(new RuntimeMeta(pk, value));
                count++;
            }
        } catch (Exception e) {
            throw new GeaflowException("jdbc query error", e.getMessage(), e);
        }
        return result;
    }

    private boolean checkTableExist(JdbcTemplate jdbcTemplate, String tableName) {
        String sql = String.format("show tables like '%s'", tableName);
        List<Map<String, Object>> tables = jdbcTemplate.queryForList(sql);
        return !tables.isEmpty();
    }

    private JdbcTemplate buildJdbcTemplate(JdbcPluginConfigClass jdbcPluginConfig) {
        String confKey = JSON.toJSONString(jdbcPluginConfig.build());
        JdbcTemplate jdbcTemplate = jdbcTemplateCache.get(confKey);
        if (jdbcTemplate != null) {
            return jdbcTemplate;
        } else {
            synchronized (lock) {
                jdbcTemplate = jdbcTemplateCache.get(confKey);
                if (jdbcTemplate != null) {
                    return jdbcTemplate;
                } else {
                    jdbcTemplate = new JdbcTemplate();
                    DataSource dataSource = getDriverManagerDataSource(jdbcPluginConfig);

                    jdbcTemplate.setDataSource(dataSource);
                    jdbcTemplateCache.put(confKey, jdbcTemplate);
                    return jdbcTemplate;
                }
            }
        }
    }

    private DriverManagerDataSource getDriverManagerDataSource(JdbcPluginConfigClass jdbcPluginConfig) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(Optional.ofNullable(jdbcPluginConfig.getDriverClass()).orElse(MYSQL_DRIVER_CLASS));
        dataSource.setUrl(jdbcPluginConfig.getUrl());
        dataSource.setUsername(jdbcPluginConfig.getUsername());
        dataSource.setPassword(jdbcPluginConfig.getPassword());
        return dataSource;
    }

    @Setter
    @Getter
    @AllArgsConstructor
    private static class RuntimeMeta {

        private String pk;

        private String value;

    }
}
