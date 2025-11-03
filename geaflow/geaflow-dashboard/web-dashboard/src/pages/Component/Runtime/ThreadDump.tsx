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
import {executeThreadDump, getThreadDumpContent} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {history, useIntl, useParams} from "@@/exports";
import {fetchComponentInfo, getByteNum, parseAgentUrl} from "@/util/CommonUtil";
import {Button, message, PaginationProps} from "antd";
import FileContentCard from "@/pages/Component/Runtime/FileContentCard";
import moment from "moment";
import {FormattedMessage} from "@umijs/max";

const DEFAULT_PAGE_SIZE_KB = 50;

const ThreadDumpContent: React.FC<{ componentName: string }> = ({
                                                                  componentName
                                                                }) => {

  const logRequestRef = useRef<API.PageRequest>({pageNo: 1, pageSize: DEFAULT_PAGE_SIZE_KB});
  const [logInfo, setLogInfo]: [API.PageResponse<API.ThreadDumpResponse>, any] = useState({total: 0} as API.PageResponse<API.ThreadDumpResponse>);
  const agentUrlRef: React.MutableRefObject<string> = useRef("undefined");
  const pidRef: React.MutableRefObject<number> = useRef(0);
  const intl = useIntl();

  const queryLog = async (agentUrl: string) => {
    let result = await getThreadDumpContent(agentUrl, logRequestRef.current.pageNo, getByteNum(logRequestRef.current.pageSize));
    setLogInfo(result.data);
  };

  const initAgentUrl = () => {
    fetchComponentInfo(componentName).then(function (componentInfo) {
      agentUrlRef.current = parseAgentUrl(componentInfo);
      pidRef.current = componentInfo?.pid ?? 0;
      queryLog(agentUrlRef.current).then();
    });
  }

  const handleThreadDumpButtonClick = async () => {
    if (pidRef.current == 0) {
      message.error("Pid is not fetched.")
      return;
    }
    message.loading(intl.formatMessage({
      id: "pages.components.thread-dump.card.regenerate.waiting",
      defaultMessage: "Start dumping, please waiting..."
    }))
    executeThreadDump(agentUrlRef.current, pidRef.current).then(function () {
      message.success(intl.formatMessage({
        id: "pages.components.thread-dump.card.regenerate.success",
        defaultMessage: "Dump success! Result is refreshed."
      }))
      queryLog(agentUrlRef.current);
    });
  }

  useEffect(initAgentUrl, []);

  const handlePaginationChange: PaginationProps['onChange'] = (page, pageSizeKB) => {
    logRequestRef.current = {pageNo: page, pageSize: pageSizeKB};
    queryLog(agentUrlRef.current);
  }

  let formattedUpdateTime = logInfo != null && logInfo.data?.lastDumpTime != null
    ? moment(logInfo.data?.lastDumpTime).format("YYYY-MM-DD HH:mm:ss")
    : intl.formatMessage({
      id: 'pages.components.thread-dump.card.emptyDumpTime',
      defaultMessage: 'No records'
    })

  return <FileContentCard
    title={intl.formatMessage({
      id: 'pages.components.thread-dump.card.description',
      defaultMessage: 'Thread Dump'
    })}
    subtitle={intl.formatMessage({
      id: 'pages.components.thread-dump.card.lastDumpTime',
      defaultMessage: 'Last dump time'
    }) + ": " + formattedUpdateTime}

    data={logInfo?.data?.content ?? ""}
    totalBytes={logInfo?.total}
    paginationRequestRef={logRequestRef}
    extra={
      <Button
        size={"small"}
        onClick={(e) => {
          e.stopPropagation();
          handleThreadDumpButtonClick();
        }}
      >
        <FormattedMessage
          id={"pages.components.thread-dump.card.reload"}
          defaultMessage={"Reload"}
        />
      </Button>
    }
    handlePaginationChange={handlePaginationChange}/>
}

const LogInfoPage: React.FC = () => {

  const pathParams = useParams();
  const componentName = pathParams.componentName;

  if (componentName == null || componentName == 'undefined') {
    history.push("/error");
    return null;
  }

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.thread-dump.page.description',
    defaultMessage: 'Show the thread-dump results of component'
  });

  return <>
    <RuntimeLayout
      tabIndex={4} // @ts-ignore
      componentName={componentName}
      description={description + " (" + componentName + ")"}
      content={<ThreadDumpContent componentName={componentName}/>}
    />
  </>
}


export default LogInfoPage;
