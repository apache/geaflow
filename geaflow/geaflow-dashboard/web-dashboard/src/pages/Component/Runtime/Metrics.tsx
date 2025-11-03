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
import {ActionType, ProCard} from "@ant-design/pro-components";
import {ProTable} from "@ant-design/pro-table/lib";
import {FormattedMessage} from "@umijs/max";
import {Progress} from "antd";
import {containerInfos, driverInfos, masterMetrics} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {useIntl, useParams} from "@@/exports";

const ProgressDiv: React.FC<{
  title: string,
  percent: number | undefined,
  description: string
}> = ({title, percent, description}) => {
  return <div style={{
    textAlign: 'center',
    // marginLeft: 'auto'
    flex: 1
  }}>
    <p style={{fontWeight: 'bold'}}>{title}</p>
    <Progress type="dashboard" percent={percent} size={200}
              strokeColor={{'0%': '#87d068', '50%': '#ffe58f', '100%': 'red'}}
              format={(percent) => `${percent}%`}
              status={'normal'}
    />
    <p>{description}</p>
  </div>
}

const MetricsChart: React.FC<{metrics: API.ProcessMetrics}> = ({metrics}) => {
  const actionRef = useRef<ActionType>();

  return (
    <>
      <ProCard>
        <div style={{display: 'flex'}}>
          <ProgressDiv
            title='JVM Heap Memory'
            percent={metrics?.heapUsedRatio}
            description={metrics?.heapUsedMB + " / " + metrics?.heapCommittedMB + " MB"}
          />
          <ProgressDiv
            title='CPU Cores'
            percent={metrics?.processCpu}
            description={metrics?.usedCores + " / " + metrics?.availCores + " Cores"}
          />
        </div>
      </ProCard>
      <ProTable<API.ProcessMetrics>
        style={{marginTop: 30}}
        headerTitle='JVM Memory'
        rowKey='totalMemoryMB'
        actionRef={actionRef}
        search={false}
        dataSource={[metrics]}
        options={false}
        pagination={false}
        columns={
          [
            {
              title: <FormattedMessage id="pages.processMetrics.totalMemoryMB" defaultMessage="Total" />,
              dataIndex: 'totalMemoryMB',
              render: (_, entity) => {
                return entity?.totalMemoryMB + " MB";
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.heapCommittedMB" defaultMessage="Heap Committed" />,
              dataIndex: 'heapCommittedMB',
              render: (_, entity) => {
                return entity?.heapCommittedMB + " MB";
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.heapUsedMB" defaultMessage="Heap Used" />,
              dataIndex: 'heapUsedMB',
              render: (_, entity) => {
                return entity?.heapUsedMB + " MB";
              },
            }
          ]
        }
      />
      <ProTable<API.ProcessMetrics>
        style={{marginTop: 30}}
        headerTitle='CPU Used'
        rowKey='availCores'
        actionRef={actionRef}
        search={false}
        dataSource={[metrics]}
        options={false}
        pagination={false}
        columns={
          [
            {
              title: <FormattedMessage id="pages.processMetrics.availCores" defaultMessage="availCores" />,
              dataIndex: 'availCores',
              render: (_, entity) => {
                return entity?.availCores;
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.usedCores" defaultMessage="usedCores" />,
              dataIndex: 'usedCores',
              render: (_, entity) => {
                return entity?.usedCores;
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.activeThreads" defaultMessage="activeThreads" />,
              dataIndex: 'activeThreads',
              render: (_, entity) => {
                return entity?.activeThreads;
              },
            }
          ]
        }
      />
      <ProTable<API.ProcessMetrics>
        style={{marginTop: 30}}
        headerTitle='Garbage Collection'
        actionRef={actionRef}
        rowKey='fgcCount'
        search={false}
        dataSource={[metrics]}
        options={false}
        pagination={false}
        columns={
          [
            {
              title: <FormattedMessage id="pages.processMetrics.fgcCount" defaultMessage="fgcCount" />,
              dataIndex: 'fgcCount',
              render: (_, entity) => {
                return entity?.fgcCount;
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.fgcTime" defaultMessage="fgcTime" />,
              dataIndex: 'fgcTime',
              render: (_, entity) => {
                return entity?.fgcTime;
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.gcCount" defaultMessage="gcCount" />,
              dataIndex: 'gcCount',
              render: (_, entity) => {
                return entity?.gcCount;
              },
            },
            {
              title: <FormattedMessage id="pages.processMetrics.gcTime" defaultMessage="gcTime" />,
              dataIndex: 'gcTime',
              render: (_, entity) => {
                return entity?.gcTime;
              },
            },
          ]
        }
      />
    </>
  )
}

const MetricsPage: React.FC = () => {

  const [metrics, setMetrics]: [API.ProcessMetrics, any] = useState({});

  const pathParams = useParams();
  const componentName = pathParams.componentName;
  const masterName = "master";

  const intl = useIntl();

  const description = intl.formatMessage({
      id: 'pages.components.metrics.page.description',
      defaultMessage: 'Show the process metrics of component'});

  useEffect(() => {
    const fetchData = async () => {
      let result;
      let metrics;
      if (masterName == componentName) {
        result = await masterMetrics();
        metrics = result.data;
      } else {
        result = componentName?.startsWith("container") ? await containerInfos() : await driverInfos();
        let component = result.data?.filter(componentInfo => componentInfo.name == componentName)
        if (component != null && component.length > 0) {
          metrics = component[0].metrics;
        }
      }
      setMetrics(metrics);
    };
    fetchData().then();
  }, []);

  return (
    // @ts-ignore
    <>
      <RuntimeLayout
        tabIndex={1}
        componentName={componentName}
        description={description + " (" + componentName + ")"}
        content={<MetricsChart metrics={metrics} />}
      />
    </>
  );
}

export default MetricsPage;
