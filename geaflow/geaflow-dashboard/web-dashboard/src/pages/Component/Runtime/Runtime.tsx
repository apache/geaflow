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

import React, {ReactNode} from 'react';
import {PageContainer} from "@ant-design/pro-components";
import {history} from '@umijs/max';
import {useIntl} from "@@/exports";

export const RuntimeLayout: React.FC<{
      tabIndex: number,
      componentName: string | undefined,
      description: string,
      content: ReactNode}
> = ({tabIndex, componentName, description, content}) => {

  const intl = useIntl();

  if (componentName == null || componentName == "undefined") {
    history.push("/error");
    return null;
  }

  let urlPrefix = '/components/' + encodeURIComponent(componentName);

  let tabMap: Record<string, {
    tab: string,
    key: string,
    url: string
  }> = {
    '1': {
      tab: intl.formatMessage({
        id: 'pages.components.metrics.page.title',
        defaultMessage: 'Metrics'
      }),
      key: '1',
      url: urlPrefix + "/metrics"
    },
    '2': {
      tab: intl.formatMessage({
        id: 'pages.components.logs.page.title',
        defaultMessage: 'Logs'
      }),
      key: '2',
      url: urlPrefix + "/logs"
    },
    '3': {
      tab: intl.formatMessage({
        id: 'pages.components.flame-graphs.page.title',
        defaultMessage: 'Flame Graphs'
      }),
      key: '3',
      url: urlPrefix + "/flame-graphs"
    },
    '4': {
      tab: intl.formatMessage({
        id: 'pages.components.thread-dump.page.title',
        defaultMessage: 'Thread Dump'
      }),
      key: '4',
      url: urlPrefix + "/thread-dump"
    },
    '5': {
      tab: intl.formatMessage({
        id: 'pages.components.configuration.page.title',
        defaultMessage: 'Configuration'
      }),
      key: '5',
      url: urlPrefix + "/configuration"
    }
  }

  // Only master will show the Configuration Tab
  if ("master" != componentName) {
    delete tabMap['5'];
  }

  if (tabMap[tabIndex.toString()] == null) {
    history.push("/error");
  }

  let header = {
    title: tabMap[tabIndex.toString()].tab,
    breadcrumb: {
      items: [
        {
          path: '',
          title: intl.formatMessage({
            id: 'menu.cluster',
            defaultMessage: 'Cluster Details'
          })
        },
        {
          title: componentName
        },
        {
          title: tabMap[tabIndex.toString()].tab
        }
      ]
    }
  }

  return (
    <PageContainer
      header={header}
      tabList={Object.values(tabMap)}
      tabActiveKey={tabIndex.toString()}
      onTabChange={(key) => history.push(tabMap[key]?.url)}
      content={description}
    >
      {content}
    </PageContainer>
)};

export default RuntimeLayout;
