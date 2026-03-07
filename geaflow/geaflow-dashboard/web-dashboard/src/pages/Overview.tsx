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
import {useIntl} from '@umijs/max';
import {Card, theme} from 'antd';
import React, {useEffect, useState} from 'react';
import {clusterOverview} from "@/services/jobs/api";
import {PipelineTable} from "@/pages/Pipeline/Pipelines";

/**
 * 每个单独的卡片，为了复用样式抽成了组件
 * @param param0
 * @returns
 */
const InfoCard: React.FC<{
  title: string;
  num: number | undefined;
  color: string;
  detailNums: DetailNum[];
}> = ({ title, num, color, detailNums }) => {
  const { useToken } = theme;

  const { token } = useToken();

  return (
    <div
      style={{
        backgroundColor: token.colorBgContainer,
        boxShadow: token.boxShadow,
        borderRadius: '8px',
        fontSize: '14px',
        color: token.colorTextSecondary,
        lineHeight: '22px',
        padding: '16px 19px',
        minWidth: '220px',
        flex: 1,
        height: '200px'
      }}
    >
      <div
        key={0}
        style={{
          display: 'flex',
          gap: '4px',
          alignItems: 'center',
        }}
      >
        <div
          style={{
            fontSize: '16px',
            color: token.colorText,
            paddingBottom: 8,
            fontWeight: 'bold'
          }}
        >
          {title}
        </div>
      </div>
      <div
        key={1}
        style={{
          marginTop: 30,
          fontSize: '30px',
          color: color,
          fontWeight: 'bold',
          textAlign: 'justify',
          lineHeight: '22px',
          marginBottom: 8,
        }}
      >
        {num ?? "-"}
      </div>
      <div
        key={2}
        style={{
        display: "inline",
        marginTop: '40px',
        textAlign: 'justify',
        float: 'left',
        lineHeight: '22px',
        fontSize: '14px'
      }}>
        {detailNums?.map((detailNum, index) => (
          <div key={index} style={{marginRight: '10px',display: "inline"}}>{detailNum.name ?? "-"}: <span style={{fontWeight: 'bold'}}>{detailNum.num ?? "-"}</span>{index % 2 == 1 ? <br/> : ""}</div>
        ))}
      </div>
    </div>
  );
};

type DetailNum = {
  name: string,
  num: number | undefined
}

const Overview: React.FC = () => {
  const [ overview, setOverview ]: [API.ClusterOverview, any] = useState({});
  const { token } = theme.useToken();
  useEffect(() => {
    const fetchData = async () => {
      let result = await clusterOverview();
      let metrics = result.data;
      setOverview(metrics);
    };
    fetchData().then();
  }, []);
  /**
   * @en-US International configuration
   * @zh-CN 国际化配置
   * */
  const intl = useIntl();
  return (
    <PageContainer
      content={intl.formatMessage({
        id: 'pages.overview.page.description',
        defaultMessage: '',
      })}
    >
      <Card
        style={{
          borderRadius: 8,
        }}
      >
        <div>
          <div
            style={{
              fontSize: '20px',
              color: token.colorTextHeading,
              marginTop: '15px',
              marginBottom: '10px'
            }}
          >
            {intl.formatMessage({
              id: 'pages.overview.page.container.card.title',
              defaultMessage: '',
            })}
          </div>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 16,
            }}
          >
            <InfoCard
              title="Active Containers"
              num={overview?.activeContainers}
              color={'LimeGreen'}
              detailNums={[
                {
                  name: 'Total Containers',
                  num: overview?.totalContainers
                },
                {
                  name: 'Active Containers',
                  num: overview?.activeContainers
                }
              ]}
            />
            <InfoCard
              title="Active Drivers"
              num={overview?.activeDrivers}
              color={'LimeGreen'}
              detailNums={[
                {
                  name: 'Total Drivers',
                  num: overview?.totalDrivers
                },
                {
                  name: 'Active Drivers',
                  num: overview?.activeDrivers
                }
              ]}
            />
            <InfoCard
              title="Available Workers"
              num={overview?.availableWorkers}
              color={'LimeGreen'}
              detailNums={[
                {
                  name: 'Total Workers',
                  num: overview?.totalWorkers
                },
                {
                  name: 'Pending Workers',
                  num: overview?.pendingWorkers
                },
                {
                  name: 'Used Workers',
                  num: overview?.usedWorkers
                },
                {
                  name: 'Available Workers',
                  num: overview?.availableWorkers
                }
              ]}
            />
          </div>
        </div>
      </Card>
      <Card style={{marginTop: 30}}>
        <PipelineTable/>
      </Card>
    </PageContainer>
  );
};

export default Overview
