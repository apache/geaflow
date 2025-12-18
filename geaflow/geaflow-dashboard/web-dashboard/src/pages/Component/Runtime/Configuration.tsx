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
import {ActionType, ProColumns} from "@ant-design/pro-components";
import {ProTable} from "@ant-design/pro-table/lib";
import {FormattedMessage} from "@umijs/max";
import {masterConfiguration} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {useIntl, useParams} from "@@/exports";
import {InputRef} from "antd";
import {getColumnSearchProps} from "@/util/TableUtil";
import {sortTable} from "@/util/CommonUtil";

const ConfigurationTable: React.FC<{data: API.MasterConfig[]}> = ({data}) => {
  const actionRef = useRef<ActionType>();
  const intl = useIntl();

  const [searchText, setSearchText] = useState('');
  const [searchedColumn, setSearchedColumn] = useState('');
  const searchInput = useRef<InputRef>(null);

  const columns: ProColumns<API.MasterConfig>[] = [
    {
      title: <FormattedMessage id="pages.componentConfiguration.key" defaultMessage="Name" />,
      dataIndex: 'name',
      ...getColumnSearchProps<API.CycleMetrics>('name', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.name, b.name)
    },
    {
      title: <FormattedMessage id="pages.componentConfiguration.value" defaultMessage="Value" />,
      dataIndex: 'value',
      ...getColumnSearchProps<API.CycleMetrics>('value', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
      sorter: (a, b) => sortTable(a.value, b.value)
    }
  ];

  return <ProTable<API.MasterConfig>
    headerTitle={intl.formatMessage({
      id: 'pages.components.configuration.table.title',
      defaultMessage: 'Master Configuration'})}
    actionRef={actionRef}
    rowKey="name"
    search={false}
    dataSource={data}
    columns={columns}
  />
}

const ConfigurationPage: React.FC = () => {

  const [masterConfigList, setMasterConfigList]: [API.MasterConfig[], any]  = useState([]);

  const pathParams = useParams();
  const componentName = pathParams.componentName;

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.configuration.page.description',
    defaultMessage: 'Show the configuration of master'});

  useEffect(() => {
    const fetchConfig = async () => {
      let result = await masterConfiguration();
      let data = result.data;
      let configList: API.MasterConfig[] = [];
      for (let key in data) {
        let config: API.MasterConfig = {
          name: key,
          value: data[key]
        }
        configList.push(config)
      }
      setMasterConfigList(configList);
    };
    fetchConfig().then();
  }, []);

  return (
    <>
      <RuntimeLayout
        tabIndex={5}
        componentName={componentName}
        description={description}
        content={<ConfigurationTable data={masterConfigList} />}
      />
    </>
  );

};

export default ConfigurationPage;
