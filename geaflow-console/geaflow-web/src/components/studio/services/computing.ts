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

import request from "./request";
import { HTTP_SERVICE_URL } from "../../constants";
import { message } from "antd";
import $i18n from "@/components/i18n";
interface ComputingParams {
  instanceId: string;
  name?: string;
}

interface GraphDefinitionParams {
  instanceName: string;
  page?: number;
  name?: string;
}

/**
 * 查询图计算列表
 */
export const getJobsList = async (params: ComputingParams) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: params,
  });

  return response;
};

/**
 * 新增图计算
 */
export const getJobsCreat = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs`, {
    method: "post",
    credentials: "include",
    withCredentials: true,
    data: params,
  });

  return response;
};

/**
 * 编辑图计算
 */
export const getJobsEdit = async (params: any, id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${id}`, {
    method: "put",
    credentials: "include",
    withCredentials: true,
    data: params,
  });

  return response;
};

/**
 * 编辑图计算数据
 */
export const getJobsEditList = async (id: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${id}`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
  });

  return response;
};

/**
 * 发布图计算
 */
export const getJobsReleases = async (jobId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/jobs/${jobId}/releases`,
    {
      method: "post",
      credentials: "include",
      withCredentials: true,
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.PublishingFailedResponsemessage",
          dm: "发布失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response;
};
export const deleteComputing = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${jobId}`, {
    method: "delete",
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.function-manage.FailedToDeleteResponsemessage",
          dm: "删除失败：{responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response;
};
export const getJobsTasks = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/tasks`, {
    method: "get",
    credentials: "include",
    withCredentials: true,
    params: { jobId },
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data?.list;
};
export const getRemoteFiles = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/remote-files`, {
    method: "get",
    requestType: "form",
  });
  return response?.data?.list;
};

export const getOlaps = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/statements`, {
    method: "get",
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data;
};

export const getOlapsResult = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/statements`, {
    method: "post",
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response?.data;
};

export const getOlapQueryId = async (olapQueryId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/statements/${olapQueryId}`,
    {
      method: "get",
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return [];
  }
  return response?.data;
};

export const deleteOlapQueryId = async (olapQueryId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/statements/${olapQueryId}`,
    {
      method: "delete",
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response;
};


export const getTablesDefinitionList = async (
  params: GraphDefinitionParams
) => {
  const { instanceName, ...others } = params;
  const response = await request(
    `${HTTP_SERVICE_URL}/api/instances/${instanceName}/tables`,
    {
      method: "get",
      params: others,
    }
  );

  if (!response?.success || !response?.data) {
    // message.error(`搜索失败: ${response?.message}`);
    return [];
  }
  return response.data?.list || [response.data];
};

export const getJobs = async (jobId: string) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/jobs/${jobId}`, {
    method: "get",
  });

  return response;
}

export const postChat = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/chats`, {
    method: "post",
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response;
};
export const postCallSync = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/chats/callSync`, {
    method: "post",
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response;
};
export const getChat = async (params: any) => {
  const response = await request(`${HTTP_SERVICE_URL}/api/chats`, {
    method: "get",
    params: params,
  });

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response;
};

export const DeleteChat = async (jobId: string) => {
  const response = await request(
    `${HTTP_SERVICE_URL}/api/chats/jobs/${jobId}`,
    {
      method: "delete",
    }
  );

  if (!response.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.computing.QueryFailedResponsemessage",
          dm: "删除失败: {responseMessage}",
        },
        { responseMessage: response.message }
      )
    );
    return "";
  }
  return response;
};
export const getllmsList = async () => {
  const response = await request(`${HTTP_SERVICE_URL}/api/llms/-/options`, {
    method: "get",
  });

  if (!response?.success) {
    message.error(
      $i18n.get(
        {
          id: "openpiece-geaflow.geaflow.services.file-manage.QueryFailedResponsemessage",
          dm: "查询失败: {responseMessage}",
        },
        { responseMessage: response?.message }
      )
    );
    return [];
  }
  return response?.data.list;

};
