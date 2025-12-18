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

import {PageContainer} from '@ant-design/pro-components';
import {FormattedMessage, useIntl} from '@umijs/max';
import {Card, theme} from 'antd';
import React, {useEffect, useState} from 'react';
import {clusterOverview} from "@/services/jobs/api";
import {jobStateEnum, JobTable} from "@/pages/Jobs";
import { Pie } from '@ant-design/charts';
import {measureTextWidth} from "@antv/util";
import { Descriptions } from 'antd/lib';

const JobCountChart: React.FC<{title: string, desc: string, data: ChartData[]}> = ({title, desc, data}) => {

  function renderStatistic(containerWidth, text, style) {
    const { width: textWidth, height: textHeight } = measureTextWidth(text, style);
    const R = containerWidth / 2; // r^2 = (w / 2)^2 + (h - offsetY)^2

    let scale = 1;

    if (containerWidth < textWidth) {
      scale = Math.min(Math.sqrt(Math.abs(Math.pow(R, 2) / (Math.pow(textWidth / 2, 2) + Math.pow(textHeight, 2)))), 1);
    }

    const textStyleStr = `width:${containerWidth}px;`;
    return `<div style="${textStyleStr};font-size:${scale}em;line-height:${scale < 1 ? 1 : 'inherit'};">${text}</div>`;
  }

  const config = {
    title: "作业数量",
    content: "状态",
    appendPadding: 10,
    data,
    angleField: 'value',
    colorField: 'type',
    color: ({ type }) => {
      return jobStateEnum[type]?.color ?? 'rgba(0, 0, 0, 0.25)'
    },
    radius: 1,
    innerRadius: 0.6,
    label: {
      type: 'inner',
      offset: '-50%',
      content: '{name}\n{value}',
      autoRotate: false,
      style: {
        textAlign: 'center',
        fontSize: 13,
      },
    },
    interactions: [
      {
        type: 'element-selected',
      },
      {
        type: 'element-active',
      },
      {
        type: 'pie-statistic-active',
      },
    ],
    statistic: {
      title: {
        offsetY: -4,
        customHtml: (container, view, datum) => {
          const {width, height} = container.getBoundingClientRect();
          const d = Math.sqrt(Math.pow(width / 2, 2) + Math.pow(height / 2, 2));
          const text = datum ? datum.type : 'Total';
          return renderStatistic(d, text, {
            fontSize: 28,
          });
        },
      },
      content: {
        offsetY: 4,
        style: {
          fontSize: '32px',
        },
        customHtml: (container, view, datum) => {
          const {width} = container.getBoundingClientRect();
          const text = datum?.value ? datum.value : data.reduce((a, b) => a + (b?.value ?? 0), 0);
          return renderStatistic(width, text, {
            fontSize: 32,
          });
        },
      },
    }
  }
  return <Pie {...config} />;
}

type ChartData = {
  type: string,
  value: number | undefined
}

const Overview: React.FC = () => {
  const [ overview, setOverview ]: [API.ClusterOverview, any] = useState({});
  const { token } = theme.useToken();
  const fetchOverviewData = async () => {
    let result = await clusterOverview();
    let overview = result.data;
    setOverview(overview);
  };
  let data: ChartData[] = [];
  let jobStateNumMap = overview?.jobStateNumMap;
  if (jobStateNumMap != null) {
    for (let key in jobStateNumMap) {
      let chartData: ChartData = {
        type: key,
        value: jobStateNumMap[key]
      };
      data.push(chartData);
    }
  }
  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();
  return (
    <PageContainer
      ghost
      header={{
        title: intl.formatMessage({
              id: 'pages.overview.page.title',
              defaultMessage: 'Overview',
            }),
        breadcrumb: {},
      }}
      content={
        <Descriptions column={2} style={{ marginBlockEnd: -16 }}>
          <Descriptions.Item label={
            intl.formatMessage({
              id: 'pages.overview.page.cluster-info.namespace',
              defaultMessage: '-',})
          }>{overview.namespace}</Descriptions.Item>
          <Descriptions.Item label={
            intl.formatMessage({
              id: 'pages.overview.page.cluster-info.master-url',
              defaultMessage: '-',})
          }>{overview.masterUrl}</Descriptions.Item>
          <Descriptions.Item label={
            intl.formatMessage({
              id: 'pages.overview.page.cluster-info.host',
              defaultMessage: '-',})
          }>{overview.host}</Descriptions.Item>
        </Descriptions>
      }
    >
      <Card
        style={{
          borderRadius: 8,
        }}
      >
        <div>
          <div
            style={{
              fontSize: '18px',
              color: token.colorTextHeading,
              marginTop: '15px',
              marginBottom: '10px'
            }}
          >
            {intl.formatMessage({
              id: 'pages.overview.page.job-count.card.title',
              defaultMessage: 'Job Count',
            })}
          </div><JobCountChart
          title="Job Counnt"
          desc="Show job count."
          data={data}
        />
        </div>
      </Card>
      <Card style={{marginTop: 30}}>
        <JobTable handleJobTableLoad={fetchOverviewData}/>
      </Card>
    </PageContainer>
  );
};

export default Overview
