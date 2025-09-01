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

package org.apache.geaflow.console.core.service;


import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.geaflow.console.common.dal.dao.ChatDao;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.dal.entity.ChatEntity;
import org.apache.geaflow.console.common.dal.model.ChatSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowStatementStatus;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.llm.GeaflowChat;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.apache.geaflow.console.core.service.converter.ChatConverter;
import org.apache.geaflow.console.core.service.converter.IdConverter;
import org.apache.geaflow.console.core.service.llm.GraphSchemaTranslator;
import org.apache.geaflow.console.core.service.llm.LLMClient;
import org.apache.geaflow.console.core.service.llm.LLMClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ChatService extends IdService<GeaflowChat, ChatEntity, ChatSearch> {

    private static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(5, 5,
        10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

    @Autowired
    private ChatDao chatDao;

    @Autowired
    private ChatConverter chatConverter;

    @Autowired
    private LLMService llmService;

    @Autowired
    private LLMClientFactory llmClientFactory;

    @Autowired
    private JobService jobService;

    private static final String HINT_STATEMENT = "使用如下图schema: ";

    private final Cache<String, String> schemaCache = CacheBuilder.newBuilder()
        .maximumSize(50)
        .expireAfterWrite(180, TimeUnit.SECONDS)
        .build();

    @Override
    protected List<GeaflowChat> parse(List<ChatEntity> entities) {
        return ListUtil.convert(entities, chatConverter::convert);
    }

    @Override
    protected IdDao<ChatEntity, ChatSearch> getDao() {
        return chatDao;
    }

    @Override
    protected IdConverter<GeaflowChat, ChatEntity> getConverter() {
        return chatConverter;
    }

    public void callASync(GeaflowChat chat, boolean withSchema) {

        final String sessionToken = ContextHolder.get().getSessionToken();
        EXECUTOR_SERVICE.submit(() -> {
            try {
                ContextHolder.init();
                ContextHolder.get().setSessionToken(sessionToken);
                callSync(chat, true, withSchema);

            } finally {
                ContextHolder.destroy();
            }
        });

    }


    public String callSync(GeaflowChat chat, boolean saveRecord, boolean withSchema) {
        String answer = null;
        GeaflowStatementStatus status = null;
        try {
            GeaflowLLM geaflowLLM = llmService.get(chat.getModelId());
            Preconditions.checkNotNull(geaflowLLM, "Model %s not found", chat.getModelId());

            LLMClient chatClient = llmClientFactory.getLLMClient(geaflowLLM.getType());

            String prompt = getPromptWithSchema(chat, withSchema);
            answer = chatClient.call(geaflowLLM, prompt);
            status = GeaflowStatementStatus.FINISHED;
            return answer;

        } catch (Exception e) {
            answer = ExceptionUtils.getStackTrace(e);
            status = GeaflowStatementStatus.FAILED;
            return answer;

        } finally {
            // whether save the query record.
            if (saveRecord) {
                chat.setAnswer(answer);
                chat.setStatus(status);
                if (chat.getId() == null) {
                    create(chat);
                } else {
                    update(chat);
                }
            }

        }
    }

    private String getPromptWithSchema(GeaflowChat chat, boolean withSchema) {
        String userPrompt = chat.getPrompt();
        if (withSchema && chat.getJobId() != null) {
            try {
                String schemaScript = schemaCache.getIfPresent(chat.getJobId());
                if (schemaScript == null) {
                    GeaflowJob job = jobService.get(chat.getJobId());
                    if (job != null && CollectionUtils.isNotEmpty(job.getGraphs())) {
                        GeaflowGraph graph = job.getGraphs().get(0);
                        schemaScript = GraphSchemaTranslator.translateGraphSchema(graph);
                        schemaCache.put(chat.getJobId(), schemaScript);
                    }
                }

                if (schemaScript == null) {
                    return userPrompt;
                }

                // add graphSchema prompt
                StringBuilder sb = new StringBuilder();
                return sb.append(HINT_STATEMENT)
                    .append(schemaScript)
                    .append("\n")
                    .append(userPrompt)
                    .toString()
                    .replace("\n", "")
                    .replaceAll("\\s+", " ");

            } catch (Exception e) {
                // return userPrompt if getting schema failed.
                log.info("Get SchemaScript failed, jobId: {}, modelId: {}", chat.getJobId(), chat.getModelId(), e);
                return userPrompt;
            }
        }

        return userPrompt;
    }

    public boolean dropByJobId(String jobId) {
        return chatDao.dropByJobId(jobId);
    }
}

