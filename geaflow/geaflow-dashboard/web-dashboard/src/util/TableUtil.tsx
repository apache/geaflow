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

import {SearchOutlined} from '@ant-design/icons';
import React from 'react';
import type {InputRef} from 'antd';
import {Button, Input, Space} from 'antd';
import type {ColumnType} from 'antd/es/table';
import type {FilterConfirmProps} from 'antd/es/table/interface';
import {DataIndex} from "rc-table/es/interface";
import {ProColumns} from "@ant-design/pro-components";
import {FormattedMessage, useIntl} from "@@/exports";

const handleSearch = (
    setSearchText: (_: string) => void,
    setSearchedColumn: (_: DataIndex) => void,
    selectedKeys: string[],
    confirm: (param?: FilterConfirmProps) => void,
    dataIndex: DataIndex,
) => {
  confirm();
  setSearchText(selectedKeys[0]);
  setSearchedColumn(dataIndex);
};

const handleReset = (setSearchText: (_: string) => void, clearFilters: () => void) => {
  clearFilters();
  setSearchText('');
};

export function getColumnSearchProps<RecordType>(dataIndex: DataIndex,
                              searchText: string,
                              searchedColumn: string,
                              searchInput: React.RefObject<InputRef>,
                              setSearchText: (_: string) => void,
                              setSearchedColumn: (_: any) => void): ProColumns<RecordType> {
  const intl = useIntl();
  return {
    filterDropdown: ({setSelectedKeys, selectedKeys, confirm, clearFilters, close}) => (
      <div style={{padding: 8}} onKeyDown={(e) => e.stopPropagation()}>
        <Input
          ref={searchInput}
          placeholder={intl.formatMessage({id: "pages.table.columns.search.placeholder", defaultMessage: "Input to search"})}
          value={selectedKeys[0]}
          onChange={(e) => setSelectedKeys(e.target.value ? [e.target.value] : [])}
          onPressEnter={() => handleSearch(setSearchText, setSearchedColumn, selectedKeys as string[], confirm, dataIndex)}
          style={{marginBottom: 8, display: 'block'}}
        />
        <Space>
          <Button
            type="primary"
            onClick={() => handleSearch(setSearchText, setSearchedColumn, selectedKeys as string[], confirm, dataIndex)}
            icon={<SearchOutlined/>}
            size="small"
            style={{width: 90}}
          >{intl.formatMessage({id: "pages.table.columns.search.search-button", defaultMessage: "Search"})}</Button>
          <Button
            onClick={() => {
              clearFilters && handleReset(setSearchText, clearFilters);
              handleSearch(setSearchText, setSearchedColumn, selectedKeys as string[], confirm, dataIndex);
            }}
            size="small"
            style={{width: 90}}
          >{intl.formatMessage({id: "pages.table.columns.search.reset-button", defaultMessage: "Reset"})}</Button>
        </Space>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <SearchOutlined style={{color: filtered ? '#1677ff' : undefined}}/>
    ),
    onFilter: (value, record) =>
      // @ts-ignore
      record[dataIndex]
      .toString()
      .toLowerCase()
      .includes((value as string).toLowerCase()),
    onFilterDropdownOpenChange: (visible) => {
      if (visible) {
        setTimeout(() => searchInput.current?.select(), 100);
      }
    }
  }
}
