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
import {containerInfos, driverInfos} from "@/services/jobs/api";
import {InputRef, Progress, TableProps} from "antd";
import {sortTable} from "@/util/CommonUtil";
import {history} from "@@/core/history";
import {getColumnSearchProps} from "@/util/TableUtil";
import {FilterValue} from "antd/es/table/interface";

const TableProgress: React.FC<
  {
    percent: number | undefined,
    used: number | undefined,
    total: number | undefined,
    unit: string | undefined
  }> = ({percent, used, total, unit}) => {
  return (
    <div>
      <Progress percent={percent}
                format={(percent) => `${percent}%`}
                strokeColor={'#108ee9'}
                status='normal'
                size="small"
                strokeLinecap="butt"
                style={{marginBottom: '1px'}}
      />
      <div style={{fontSize: '80%'}}>{used} / {total} {unit}</div>
    </div>
  )
}

const ComponentInfoList: React.FC<{componentType: string}> = ({componentType}) => {

  const actionRef = useRef<ActionType>();
  const [selectedRowsState, setSelectedRows] = useState<API.ComponentInfo[]>([]);

  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();

  const [searchText, setSearchText] = useState('');
  const [searchedColumn, setSearchedColumn] = useState('');
  const [filteredInfo, setFilteredInfo] = useState<Record<string, API.ComponentInfo | null>>({});
  const searchInput = useRef<InputRef>(null);

  const handleChange: TableProps<API.ComponentInfo>['onChange'] = (pagination, filters, sorter) => {
    setFilteredInfo(filters as Record<string, API.ComponentInfo | null>);
  };

  const componentActiveEnum = {
    true: {
      text: (<FormattedMessage id="pages.componentTable.componentState.active"
                               defaultMessage="Active"/>),
      status: 'Success',
    },
    false: {
      text: (<FormattedMessage id="pages.componentTable.componentState.timeout"
                               defaultMessage="Timeout"/>),
      status: 'Error',
    },
  };

  const columns: ProColumns<API.ComponentInfo>[] = [
    {
      title: <FormattedMessage id="pages.componentTable.id" defaultMessage="Id"/>,
      dataIndex: 'id',
      tip: 'The id is the unique key',
      ...getColumnSearchProps<API.CycleMetrics>('id', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.id, b.id),
      defaultSortOrder: 'ascend'
    },
    {
      title: <FormattedMessage id="pages.componentTable.name" defaultMessage="Name"/>,
      dataIndex: 'name',
      ...getColumnSearchProps<API.CycleMetrics>('name', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.name, b.name),
      render: (dom, entity) => {
        return (
          <a
            onClick={() => { // @ts-ignore
              history.push('/components/' + encodeURIComponent(entity?.name) + '/metrics')}}
          >
            {dom}
          </a>
        );
      },
    },
    {
      title: <FormattedMessage id="pages.componentTable.lastTimestamp" defaultMessage="Last Heartbeat" />,
      dataIndex: 'lastTimestamp',
      valueType: 'dateTime',
      sorter: (a, b) => sortTable(a.lastTimestamp, b.lastTimestamp)
    },
    {
      title: <FormattedMessage id="pages.componentTable.isActive" defaultMessage="Heartbeat Status" />,
      dataIndex: 'isActive',
      sorter: (a, b) => sortTable(a.isActive, b.isActive),
      valueEnum: componentActiveEnum,
      filters: [
        { text: intl.formatMessage({id: 'pages.componentTable.componentState.active', defaultMessage: 'Actve'}), value: true },
        { text: intl.formatMessage({id: 'pages.componentTable.componentState.timeout', defaultMessage: 'Timeout'}), value: false },
      ],
      filteredValue: (filteredInfo.isActive || null) as FilterValue,
      onFilter: (value, record) => value == record.isActive,
    },
    {
      title: <FormattedMessage id="pages.componentTable.host" defaultMessage="Host" />,
      dataIndex: 'host',
      ...getColumnSearchProps<API.CycleMetrics>('host', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.host, b.host)
    },
    {
      title: <FormattedMessage id="pages.componentTable.pid" defaultMessage="Pid" />,
      dataIndex: 'pid',
      ...getColumnSearchProps<API.CycleMetrics>('pid', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.pid, b.pid)
    },
    {
      title: <FormattedMessage id="pages.componentTable.gcCount" defaultMessage="GC Count" />,
      dataIndex: 'gcCount',
      sorter: (a, b) => sortTable(a.fgcCount, b.fgcCount),
    },
    {
      title: <FormattedMessage id="pages.componentTable.fgcCount" defaultMessage="FGC Count" />,
      dataIndex: 'fgcCount',
      sorter: (a, b) => sortTable(a.fgcCount, b.fgcCount),
    },
    {
      title: <FormattedMessage id="pages.componentTable.activeThreads" defaultMessage="Active Threads" />,
      dataIndex: 'activeThreads',
    },
    {
      title: <FormattedMessage id="pages.componentTable.heapUsedRatio" defaultMessage="Heap Used" />,
      dataIndex: 'heapUsedRatio',
      sorter: (a, b) => sortTable(a.heapUsedRatio, b.heapUsedRatio),
      render: (_, entity) => {
        return (
        <TableProgress percent={entity.heapUsedRatio}
                        used={entity.heapUsedMB}
                        total={entity.heapCommittedMB}
                        unit={"MB"}
        />
        )
      },
    },
    {
      title: <FormattedMessage id="pages.componentTable.processCpu" defaultMessage="Cpu Used" />,
      dataIndex: 'processCpu',
      sorter: (a, b) => sortTable(a.processCpu, b.processCpu),
      render: (_, entity) => {
        return (
          <TableProgress percent={entity.processCpu}
                         used={entity.usedCores}
                         total={entity.availCores}
                         unit={"Cores"}
          />
        )
      },
    },
  ];

  const request = componentType === 'driver' ? driverInfos : containerInfos;

  return (
    <PageContainer
      content={intl.formatMessage({
      id: 'pages.' + componentType + 'Table.page.description',
      defaultMessage: '',
    })}
    >
      <ProTable<API.ComponentInfo>
        headerTitle={intl.formatMessage({
          id: 'pages.' + componentType + 'Table.page.title',
          defaultMessage: '',
        })}
        onChange={handleChange}
        actionRef={actionRef}
        rowKey="id"
        search={false}
        request={request}
        columns={columns}
        postData={(data: API.ComponentInfo[]) => {
          for (let datum of data) {
            datum.heapCommittedMB = datum.metrics?.heapCommittedMB;
            datum.heapUsedMB = datum.metrics?.heapUsedMB;
            datum.heapUsedRatio = datum.metrics?.heapUsedRatio;
            datum.totalMemoryMB = datum.metrics?.totalMemoryMB;
            datum.fgcCount = datum.metrics?.fgcCount;
            datum.fgcTime = datum.metrics?.fgcTime;
            datum.gcTime = datum.metrics?.gcTime;
            datum.gcCount = datum.metrics?.gcCount;
            datum.avgLoad = datum.metrics?.avgLoad;
            datum.availCores = datum.metrics?.availCores;
            datum.processCpu = datum.metrics?.processCpu;
            datum.usedCores = datum.metrics?.usedCores;
            datum.activeThreads = datum.metrics?.activeThreads;
          }
          return data;
        }}
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

export default ComponentInfoList;
