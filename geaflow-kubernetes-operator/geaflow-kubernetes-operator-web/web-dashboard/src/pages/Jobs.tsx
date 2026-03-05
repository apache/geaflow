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

import type {ActionType, ProColumns} from '@ant-design/pro-components';
import {FooterToolbar, PageContainer, ProTable} from '@ant-design/pro-components';
import {FormattedMessage, useIntl} from '@umijs/max';
import React, {useRef, useState} from 'react';
import {jobList} from "@/services/jobs/api";
import moment from "moment";
import {useParams} from "@@/exports";
import {sortTable} from "@/util/CommonUtil";
import {Modal} from 'antd';
import {Controlled as CodeMirror} from 'react-codemirror2';
import 'codemirror/lib/codemirror.js'
import 'codemirror/lib/codemirror.css'
import 'codemirror/theme/material.css';
import 'codemirror/addon/display/autorefresh'
import 'codemirror/addon/selection/active-line';
import 'codemirror/mode/yaml/yaml';
import yaml from 'js-yaml'


export const JobTable: React.FC<{handleJobTableLoad: Function | null}> = ({handleJobTableLoad}) => {

  const actionRef = useRef<ActionType>();
  const [selectedRowsState, setSelectedRows] = useState<API.GeaflowJob[]>([]);


  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();

  const pathParams = useParams();
  const pipelineName = pathParams.pipelineName;
  const columns: ProColumns<API.GeaflowJob>[] = [
    {
      title: "Unique id",
      dataIndex: ['status', 'jobUid'],
      sorter: (a, b) => sortTable(a.status?.jobUid, b.status?.jobUid)
    },
    {
      title: (
        <FormattedMessage
          id="pages.jobTable.name"
          defaultMessage="Name"
        />
      ),
      dataIndex: ['metadata', 'name'],
      sorter: (a, b) => sortTable(a.metadata?.name, b.metadata?.name)
    },
    {
      title: <FormattedMessage id="pages.jobTable.submitTime" defaultMessage="Submit Time"/>,
      dataIndex: ['metadata', 'creationTimestamp'],
      sorter: (a, b) => sortTable(a.metadata?.creationTimestamp, b.metadata?.creationTimestamp),
      render: (_, entity) => {
        return moment(entity.metadata?.creationTimestamp).format('YYYY-MM-DD HH:mm:ss');
      },
    },
    {
      title: <FormattedMessage id="pages.jobTable.state" defaultMessage="Job State"/>,
      dataIndex: ['status', 'state'],
      sorter: (a, b) => sortTable(a.status?.state, b.status?.state),
      valueEnum: jobStateEnum
    },
    {
      title: <FormattedMessage id="pages.jobTable.clientState" defaultMessage="Client State"/>,
      dataIndex: ['status', 'clientState'],
      sorter: (a, b) => sortTable(a.status?.clientState, b.status?.clientState),
      valueEnum: componentStateEnum
    },
    {
      title: <FormattedMessage id="pages.jobTable.masterState" defaultMessage="Master State"/>,
      dataIndex: ['status', 'masterState'],
      sorter: (a, b) => sortTable(a.status?.masterState, b.status?.masterState),
      valueEnum: componentStateEnum
    },
    {
      title: <FormattedMessage id="pages.jobTable.driverNum" defaultMessage="Driver Num"/>,
      dataIndex: ['spec', 'driverSpec', 'driverNum'],
      sorter: (a, b) => sortTable(a.spec?.driverSpec?.driverNum, b.spec?.driverSpec?.driverNum)
    },
    {
      title: <FormattedMessage id="pages.jobTable.containerNum" defaultMessage="Container Num"/>,
      dataIndex: ['spec', 'containerSpec', 'containerNum'],
      sorter: (a, b) => sortTable(a.spec?.containerSpec?.containerNum, b.spec?.containerSpec?.containerNum)
    },
    {
      title: <FormattedMessage id="pages.jobTable.errorMessage" defaultMessage="Error Message"/>,
      dataIndex: ['status', 'errorMessage'],
      sorter: false,
      render: (_, entity) => {
        return entity?.status?.errorMessage ? <a
          key="errorMsg"
          onClick={() => {
            Modal.warning({
              title: <FormattedMessage id="pages.jobTable.errorMessage"
                                       defaultMessage="Error Message"/>,
              content: entity?.status?.errorMessage
            });
          }}
        >
          <FormattedMessage id="pages.jobTable.errorMessage.button.name" defaultMessage="Show"/>
        </a> : '-'
      }
    },
    {
      title: <FormattedMessage id="pages.jobTable.operation" defaultMessage="Operation"/>,
      dataIndex: ['operation'],
      sorter: false,
      render: (_, entity) => {
        return [
          <a
            key="details"
            onClick={() => {
              Modal.info({
                title: <FormattedMessage id="pages.jobTable.details" defaultMessage="Details"/>,
                width: '1000px',
                content: <CodeMirror
                  value={yaml.dump(entity)}
                  options={{
                    mode: 'yaml',
                    theme: 'material',
                    lineNumbers: true, // 是否显示行号
                    readOnly: true,  // 是否只读
                    lineWiseCopyCut: true,
                    autofocus: true,
                    lineWrapping: true,
                    smartIndent: true,
                    lint: true,
                    autoRefresh: true,
                    gutters: ['CodeMirror-lint-markers'],
                  }}
                  editorDidMount={(editor) => {
                    editor.setSize('900px', '650px');
                  }}
                  onChange={(editor: any, data: any, value: string) => {
                  }}
                  onBeforeChange={(editor: any, data: any, value: string) => {
                  }}
                />
              });
            }}
          >
            <FormattedMessage id="pages.jobTable.operation.details" defaultMessage="Details"/>
          </a>
        ]
      }
    }
  ];
  return <>
    <ProTable<API.GeaflowJob, { pipelineName: string }>
      headerTitle={intl.formatMessage({
        id: 'pages.jobTable.page.title',
        defaultMessage: 'Job List',
      })}
      actionRef={actionRef}
      search={false}
      rowKey={record => record?.metadata?.name ?? ""}
      params={{// @ts-ignore
        pipelineName: encodeURIComponent(pipelineName)
      }}
      request={jobList}
      columns={columns}
      rowSelection={{
        onChange: (_, selectedRows) => {
          setSelectedRows(selectedRows);
        },
      }}
      onLoad={(dataSource) => {
        if (handleJobTableLoad != null) {
          handleJobTableLoad();
        }
      }}
    />
    {selectedRowsState?.length > 0 && (
      <FooterToolbar
        extra={
          <div>
            <FormattedMessage id="pages.jobTable.chosen" defaultMessage="Chosen"/>{' '}
            <a style={{fontWeight: 600}}>{selectedRowsState.length}</a>{' '}
            <FormattedMessage id="pages.jobTable.item" defaultMessage="项"/>
          </div>
        }
      >
      </FooterToolbar>
    )}
  </>
}

export const jobStateEnum = {
  INIT: {
    text: (<FormattedMessage id="pages.jobTable.jobState.init" defaultMessage="Init"/>),
    status: 'Processing',
    color: '#1890ff'
  },
  SUBMITTED: {
    text: (<FormattedMessage id="pages.jobTable.jobState.submitted" defaultMessage="Submitted"/>),
    status: 'Processing',
    color: '#1890ff'
  },
  RUNNING: {
    text: (<FormattedMessage id="pages.jobTable.jobState.running" defaultMessage="Running"/>),
    status: 'Processing',
    color: '#52c41a'
  },
  FAILED: {
    text: (<FormattedMessage id="pages.jobTable.jobState.failed" defaultMessage="Failed"/>),
    status: 'Error',
    color: '#ff4d4f'
  },
  SUSPENDED: {
    text: (<FormattedMessage id="pages.jobTable.jobState.suspended" defaultMessage="Suspended"/>),
    status: 'Default',
    color: 'rgba(0, 0, 0, 0.25)'
  },
  REDEPLOYING: {
    text: (
      <FormattedMessage id="pages.jobTable.jobState.redeploying" defaultMessage="Redeploying"/>),
    status: 'Processing',
    color: '#9370DB'
  },
  FINISHED: {
    text: (<FormattedMessage id="pages.jobTable.jobState.finished" defaultMessage="Finished"/>),
    status: 'Success',
    color: '#3CB371'
  },
};

export const componentStateEnum = {
  NOT_DEPLOYED: {
    text: (<FormattedMessage id="pages.jobTable.componentState.not-deployed"
                             defaultMessage="Not-deployed"/>),
    status: 'Default',
  },
  DEPLOYED_NOT_READY: {
    text: (<FormattedMessage id="pages.jobTable.componentState.deployed-not-ready"
                             defaultMessage="Deployed-not-ready"/>),
    status: 'Processing',
  },
  RUNNING: {
    text: (<FormattedMessage id="pages.jobTable.componentState.running" defaultMessage="Running"/>),
    status: 'Processing',
  },
  EXITED: {
    text: (<FormattedMessage id="pages.jobTable.componentState.exited" defaultMessage="Exited"/>),
    status: 'Default',
  },
  ERROR: {
    text: (<FormattedMessage id="pages.jobTable.componentState.error" defaultMessage="Error"/>),
    status: 'Error'
  },
};

const JobList: React.FC = () => {
  const intl = useIntl();
  return (
    <PageContainer
      content={intl.formatMessage({
        id: 'pages.jobTable.page.description',
        defaultMessage: 'Show the list of jobs in this cluster.',
      })}
    >
      <JobTable handleJobTableLoad={null}/>
    </PageContainer>
  );
};

export default JobList
