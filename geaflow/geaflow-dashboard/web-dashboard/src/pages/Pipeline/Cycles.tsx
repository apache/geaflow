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
import {FormattedMessage, history, useIntl} from '@umijs/max';
import {getColumnSearchProps} from '@/util/TableUtil';
import React, {useRef, useState} from 'react';
import {cycleList} from "@/services/jobs/api";
import moment from "moment";
import {useParams} from "@@/exports";
import {sortTable} from "@/util/CommonUtil";
import {InputRef} from 'antd';

const CycleMetricList: React.FC = () => {

  const actionRef = useRef<ActionType>();
  const [selectedRowsState, setSelectedRows] = useState<API.CycleMetrics[]>([]);


  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();

  const pathParams = useParams();
  const pipelineName = pathParams.pipelineName;

  if (pipelineName == null || pipelineName == "undefined") {
    history.push("/error");
    return null;
  }

  const [searchText, setSearchText] = useState('');
  const [searchedColumn, setSearchedColumn] = useState('');
  const searchInput = useRef<InputRef>(null);

  const columns: ProColumns<API.CycleMetrics>[] = [
    {
      title: (
        <FormattedMessage
          id="pages.cycleTable.name"
          defaultMessage="Cycle name"
        />
      ),
      dataIndex: 'name',
      ...getColumnSearchProps<API.CycleMetrics>('name', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.name, b.name)
    },
    {
      title: <FormattedMessage id="pages.cycleTable.opName" defaultMessage="Opeartor Name" />,
      dataIndex: 'opName',
      ...getColumnSearchProps<API.CycleMetrics>('opName', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.opName, b.opName)
    },
    {
      title: <FormattedMessage id="pages.cycleTable.duration" defaultMessage="Duration" />,
      dataIndex: 'duration',
      sorter: (a, b) => sortTable(a.duration, b.duration),
      render: (_, entity) => {
        return entity.duration + " ms";
      },
    },
    {
      title: <FormattedMessage id="pages.cycleTable.startTime" defaultMessage="Start Time" />,
      dataIndex: 'startTime',
      valueType: 'dateTime',
      defaultSortOrder: 'descend',
      sorter: (a, b) => sortTable(a.startTime, b.startTime)
    },
    {
      title: <FormattedMessage id="pages.cycleTable.totalTasks" defaultMessage="Total Task Num" />,
      dataIndex: 'totalTasks',
      sorter: false
    },
    {
      title: <FormattedMessage id="pages.cycleTable.inputRecords" defaultMessage="Input Records" />,
      dataIndex: 'inputRecords',
      sorter: false
    },
    {
      title: <FormattedMessage id="pages.cycleTable.outputRecords" defaultMessage="Output Records" />,
      dataIndex: 'outputRecords',
      sorter: false
    },
    {
      title: <FormattedMessage id="pages.cycleTable.inputKb" defaultMessage="Input Size" />,
      dataIndex: 'inputKb',
      sorter: false,
      render: (_, entity) => {
        return entity.inputKb + " KB";
      },
    },
    {
      title: <FormattedMessage id="pages.cycleTable.outputKb" defaultMessage="Output Size" />,
      dataIndex: 'outputKb',
      sorter: false,
      render: (_, entity) => {
        return entity.outputKb + " KB";
      },
    },
    {
      title: <FormattedMessage id="pages.cycleTable.avgExecuteTime" defaultMessage="Avg Execute Time" />,
      dataIndex: 'avgExecuteTime',
      sorter: false
    },
  ];

  let description = intl.formatMessage({
      id: 'pages.cycleTable.page.description',
      defaultMessage: 'Show the cycles of a pipeline.',
    }) + " (" + decodeURIComponent(pipelineName) + ")"

  let header = {
    title: intl.formatMessage({
      id: 'pages.cycleTable.page.title',
      defaultMessage: 'Cycle Metrics List',
    }),
    breadcrumb: {
      items: [
        {
          path: '/pipelines',
          title: intl.formatMessage({
            id: 'menu.pipeline',
            defaultMessage: 'Pipelines'
          })
        },
        {
          title: decodeURIComponent(pipelineName)
        },
        {
          title: intl.formatMessage({
            id: 'menu.pipeline.cycles',
            defaultMessage: 'Cycles'
          })
        }
      ]
    }
  }

  return (
    <PageContainer
      header={header}
      content={description}
    >
      <ProTable<API.CycleMetrics, {pipelineName: string}>
        headerTitle={intl.formatMessage({
          id: 'pages.cycleTable.page.title',
          defaultMessage: 'Cycle Metrics List',
        })}
        actionRef={actionRef}
        search={false}
        rowKey="name"
        params={{
          pipelineName: encodeURIComponent(pipelineName)}}
        request={async (params: {
          pipelineName: string;
        }) => {
          return cycleList(params.pipelineName);
        }}
        columns={columns}
        rowSelection={{
          onChange: (_, selectedRows) => {
            setSelectedRows(selectedRows);
          },
        }}
      />
      {selectedRowsState?.length > 0 && (
        <FooterToolbar
          extra={
            <div>
              <FormattedMessage id="pages.searchTable.chosen" defaultMessage="Chosen" />{' '}
              <a style={{ fontWeight: 600 }}>{selectedRowsState.length}</a>{' '}
              <FormattedMessage id="pages.searchTable.item" defaultMessage="项" />
            </div>
          }
        >
        </FooterToolbar>
      )}
    </PageContainer>
  );
};

export default CycleMetricList;
