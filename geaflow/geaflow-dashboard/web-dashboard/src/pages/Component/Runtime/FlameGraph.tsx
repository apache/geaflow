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

import React, {useEffect, useRef, useState} from 'react';
import {ActionType, ModalForm, ProFormDigit, ProFormSelect} from "@ant-design/pro-components";
import {ProTable} from "@ant-design/pro-table/lib";
import {FormattedMessage} from "@umijs/max";
import {deleteFlameGraph, executeFlameGraph, flameGraphList} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {history, useIntl, useParams} from "@@/exports";
import {fetchComponentInfo, formatFileSize, parseAgentUrl, sortTable} from "@/util/CommonUtil";
import {Button, message, Popconfirm} from 'antd';
import {PlusOutlined} from "@ant-design/icons";

const FlameGraphTable: React.FC<{
  componentName: string,
  agentUrl: string | undefined,
  pid: number | undefined
}> = ({componentName, agentUrl, pid}) => {
  const actionRef = useRef<ActionType>();
  const intl = useIntl();

  const [createModalOpen, handleCreateModalOpen] = useState<boolean>(false);

  const handleAdd = async (request: API.FlameGraphRequest) => {
    const hide = message.loading(intl.formatMessage({
      id: 'pages.components.flame-graphs.table.createForm.loadingMessage',
      defaultMessage: 'Executing...'
    }));
    try {
      request.pid = pid
      // @ts-ignore
      const result = await executeFlameGraph(agentUrl, request);
      hide();
      if (result.success) {
        message.success(intl.formatMessage({
          id: 'pages.components.flame-graphs.table.createForm.successMessage',
          defaultMessage: 'Start executing in backend, please wait for the duration you input' +
            ' and refresh table.'
        }));
        return true;
      }
      throw new Error(result.message);
    } catch (error) {
      hide();
      let errorMessage = intl.formatMessage({
        id: 'pages.components.flame-graphs.table.createForm.errorMessage',
        defaultMessage: 'Executed Failed.'
      });
      message.error(errorMessage);
      return false;
    }
  };

  const handleDelete = async (path: string | undefined) => {
    if (path == undefined) {
      message.error("Path is not found.");
      return false;
    }
    const hide = message.loading(intl.formatMessage({
      id: 'pages.components.flame-graphs.table.deleteForm.loadingMessage',
      defaultMessage: 'Deleting...'
    }));
    try {
      // @ts-ignore
      const result = await deleteFlameGraph(agentUrl, path);
      hide();
      console.log(result)
      if (result.success) {
        message.success(intl.formatMessage({
          id: 'pages.components.flame-graphs.table.deleteForm.successMessage',
          defaultMessage: 'Deleted Successfully, refreshing table list.'
        }));
        return true;
      }
      throw new Error(result.message);
    } catch (error) {
      hide();
      let errorMessage = intl.formatMessage({
        id: 'pages.components.flame-graphs.table.deleteForm.errorMessage',
        defaultMessage: 'Delete Failed.'
      });
      message.error(errorMessage);
      return false;
    }
  };


  return <>
    <ProTable<API.FileInfo>
      headerTitle={intl.formatMessage({
        id: 'pages.components.flame-graphs.table.description',
        defaultMessage: 'FlameGraph History Files'
      })}
      actionRef={actionRef}
      rowKey="path"
      search={false}
      toolBarRender={() => [
        <Button
          type="primary"
          key="primary"
          onClick={() => {
            handleCreateModalOpen(true);
          }}
        >
          <PlusOutlined/> <FormattedMessage id="pages.components.flame-graphs.table.new"
                                            defaultMessage="New"/>
        </Button>,
      ]}
      params={{agentUrl}}
      // @ts-ignore
      request={agentUrl == null ? null : async (params: {
        agentUrl: string;
      }) => {
        return flameGraphList(params.agentUrl);
      }}
      columns={
        [
          {
            title: <FormattedMessage id="pages.components.flame-graphs.table.createdTime"
                                     defaultMessage="Created Time"/>,
            dataIndex: 'createdTime',
            valueType: 'dateTime',
            sorter: (a, b) => sortTable(a.createdTime, b.createdTime),
            defaultSortOrder: 'descend'
          },
          {
            title: <FormattedMessage id="pages.components.flame-graphs.table.path"
                                     defaultMessage="Path"/>,
            dataIndex: 'path',
            sorter: (a, b) => sortTable(a.path, b.path),
            render: (dom, entity) => {
              return (
                <a
                  onClick={() => { // @ts-ignore
                    history.push('/components/' + encodeURIComponent(componentName) + '/flame-graphs/' + encodeURIComponent(entity?.path))}}
                >
                  {dom}
                </a>
              );
            },
          },
          {
            title: <FormattedMessage id="pages.components.flame-graphs.table.size"
                                     defaultMessage="Size"/>,
            dataIndex: 'size',
            sorter: (a, b) => sortTable(a.size, b.size),
            render: (dom, entity) => {
              return (formatFileSize(entity.size));
            },
          },
          {
            title: <FormattedMessage id="pages.components.flame-graphs.table.operation"
                                     defaultMessage="Operation"/>,
            dataIndex: 'operation',
            valueType: 'option',
            render: (dom, entity) => {
              return (
                [
                  <Popconfirm
                    title={intl.formatMessage({
                      id: 'pages.components.flame-graphs.table.operation.delete.confirmMessage',
                      defaultMessage: 'Are you sure to delete this file?'
                    })}
                    onConfirm={async () => {
                      const successs = await handleDelete(entity?.path);
                      if (successs) {
                        if (actionRef.current) {
                          actionRef.current.reload();
                        }
                      }
                    }}
                    okText={intl.formatMessage({
                      id: 'pages.pop-confirm.yes',
                      defaultMessage: 'Yes'
                    })}
                    cancelText={intl.formatMessage({
                      id: 'pages.pop-confirm.no',
                      defaultMessage: 'No'
                    })}
                  >
                    <a key="delete">
                      <FormattedMessage
                        id="pages.components.flame-graphs.table.operation.delete"
                        defaultMessage="Delete"/>
                    </a>
                  </Popconfirm>
                ]
              );
            },
          },
        ]
      }
    />
    <ModalForm
      title={intl.formatMessage({
        id: 'pages.components.flame-graphs.table.createForm.new',
        defaultMessage: 'New Flame Graph',
      })}
      width="400px"
      open={createModalOpen}
      onOpenChange={handleCreateModalOpen}
      onFinish={async (value) => {
        console.log(value)
        const success = await handleAdd(value as API.FlameGraphRequest);
        if (success) {
          handleCreateModalOpen(false);
          if (actionRef.current) {
            actionRef.current.reload();
          }
        }
      }}
    >
      <ProFormSelect
        name="type"
        label={intl.formatMessage({
          id: 'pages.components.flame-graphs.table.createForm.profilerTypeLabel',
          defaultMessage: 'Profiler Type',
        })}
        valueEnum={{
          CPU: 'CPU',
          ALLOC: 'ALLOC',
        }}
        width="md"
        rules={[
          {
            required: true
          },
        ]}
      />
      <ProFormDigit
        name="duration"
        label={intl.formatMessage({
          id: 'pages.components.flame-graphs.table.createForm.durationLabel',
          defaultMessage: 'Profiler Time (duration)',
        })}
        width="md"
        min={1}
        max={60}
        fieldProps={{ precision: 0 }}
        rules={[
          {
            required: true,
            message: (
              <FormattedMessage
                id="pages.components.flame-graphs.table.createForm.durationContent"
                defaultMessage="Please input how many duration you want to profiler. (among 1 ~ 60)"
              />
            ),
          },
        ]}
      />
    </ModalForm>
  </>
}

const FlameGraphsPage: React.FC = () => {
  const [agentUrl, setAgentUrl]: [string | undefined, any] = useState(undefined);
  const pidRef: React.MutableRefObject<number | undefined> = useRef(undefined);

  const pathParams = useParams();
  const componentName = pathParams.componentName;
  if (componentName == null || componentName == 'undefined') {
    history.push("/error");
    return null;
  }

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.flame-graphs.page.description',
    defaultMessage: 'Show the flame-graphs of component.'
  });

  useEffect(() => {
    fetchComponentInfo(componentName).then(function (component) {
      setAgentUrl(parseAgentUrl(component));
      pidRef.current = component?.pid;
    });
  }, []);

  return <>
    <RuntimeLayout
      tabIndex={3} // @ts-ignore
      componentName={componentName}
      description={description + " (" + componentName + ")"}
      content={<FlameGraphTable componentName={componentName} agentUrl={agentUrl} pid={pidRef.current}/>}
    />
  </>
}


export default FlameGraphsPage;
