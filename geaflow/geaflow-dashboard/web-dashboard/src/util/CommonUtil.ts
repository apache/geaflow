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

import {containerInfos, driverInfos, masterInfo} from "@/services/jobs/api";

export function sortTable<T = string | number | boolean | undefined>(a: T, b: T): number {
  if (a == undefined || b == undefined) {
    return 0
  }
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  } else if (typeof a === 'boolean' && typeof b === 'boolean') {
    return a ? 1 : -1
  } else {
    return a > b ? 1 : -1
  }
}

export const fetchComponentInfo = async (componentName: string): Promise<API.ComponentInfo | undefined> => {
  let result;
  let component = undefined;
  if ("master" == componentName) {
    result = await masterInfo();
    component = result.data;
  } else {
    result = componentName?.startsWith("container") ? await containerInfos() : await driverInfos();
    let components = result.data?.filter(componentInfo => componentInfo.name == componentName)
    if (components != null && components.length > 0) {
      component = components[0];
    }
  }
  return component;
};

export const parseAgentUrl = (component: API.ComponentInfo | undefined): string => {
  if (component != null) {
    return component?.host + ":" + component?.agentPort;
  }
  return "undefined";
}

export const formatFileSize = (size: number | undefined) => {
  if(null == size || 0 == size){
    return "0 Byte";
  }
  const UNITS = ["Byte","KB","MB","GB"];
  let index = Math.floor(Math.log(size) / Math.log(1024));
  let fmtSize = size / Math.pow(1024, index);
  let fmtSizeStr;
  if(fmtSize % 1 === 0) {
    fmtSizeStr = fmtSize.toFixed(0); //如果是整数不保留小数
  } else {
    fmtSizeStr = fmtSize.toFixed(2);//保留的小数位数
  }
  return fmtSizeStr + " " + UNITS[index];
}

export function getByteNum(kbNum: number) {
  return kbNum * 1024
}
