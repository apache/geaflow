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
import React, {useRef, useState} from 'react';
import {pipelineList} from "@/services/jobs/api";
import moment from 'moment';
import {sortTable} from "@/util/CommonUtil";
import {InputRef} from "antd";
import {getColumnSearchProps} from "@/util/TableUtil";

export const PipelineTable: React.FC = () => {

  const [selectedRowsState, setSelectedRows] = useState<API.PipelineMetrics[]>([]);
  const actionRef = useRef<ActionType>();
  const intl = useIntl();

  const [searchText, setSearchText] = useState('');
  const [searchedColumn, setSearchedColumn] = useState('');
  const searchInput = useRef<InputRef>(null);

  const columns: ProColumns<API.PipelineMetrics>[] = [
    {
      title: (
        <FormattedMessage
          id="pages.pipelineTable.name"
          defaultMessage="Pipeline name"
        />
      ),
      dataIndex: 'name',
      sorter: (a, b) => sortTable(a.name, b.name),
      ...getColumnSearchProps<API.CycleMetrics>('name', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      render: (dom, entity) => {
        return (
          <a
            onClick={() => { // @ts-ignore
              history.push('/pipelines/' + encodeURIComponent(entity?.name) + '/cycles')}}
          >
            {dom}
          </a>
        );
      },
    },
    {
      title: <FormattedMessage id="pages.pipelineTable.startTime" defaultMessage="Start Time" />,
      dataIndex: 'startTime',
      valueType: 'dateTime',
      defaultSortOrder: 'descend',
      sorter: (a, b) => sortTable(a.startTime, b.startTime),
    },
    {
      title: <FormattedMessage id="pages.pipelineTable.duration" defaultMessage="Duration" />,
      dataIndex: 'duration',
      sorter: (a, b) => sortTable(a.duration, b.duration),
      render: (_, entity) => {
        return entity.duration + " ms";
      },
    }
  ];
  return (
    <>
      <ProTable<API.PipelineMetrics>
        headerTitle={intl.formatMessage({
          id: 'pages.pipelineTable.page.title',
          defaultMessage: 'Pipeline Metrics List',
        })}
        actionRef={actionRef}
        rowKey="name"
        search={false}
        request={pipelineList}
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
    </>
  )
}

const PipelineMetricList: React.FC = () => {

  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();
  return (
    <PageContainer
      content={intl.formatMessage({
      id: 'pages.pipelineTable.page.description',
      defaultMessage: '',
    })}
    >
      <PipelineTable/>
    </PageContainer>
  );
};

export default PipelineMetricList;
