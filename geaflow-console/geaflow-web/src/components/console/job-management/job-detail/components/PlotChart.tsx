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

import React, { useEffect } from "react";
import { Area, Line } from "@antv/g2plot";

interface PlotChartProps {
  xField: string;
  // 图形在 x 方向对应的数据字段名
  yField: string;
  // 图形在 y 方向对应的数据字段名
  seriesField: string;
  // 分组字段
  data: any;
  // 图标数据
}

export const PlotChart: React.FC<PlotChartProps> = ({
  xField,
  yField,
  seriesField,
  data,
}) => {
  useEffect(() => {
    const area = new Area("container", {
      data,
      xField,
      yField,
      seriesField,
    });
    area.render();

    const line = new Line("containerLine", {
      data,
      padding: "auto",
      xField,
      yField,
      xAxis: {
        tickCount: 5,
      },
    });

    line.render();
  }, []);

  return (
    <div
      style={{
        width: "100%",
        display: "flex",
        justifyContent: "space-around",
      }}
    >
      <div id="container"></div>
      <div id="containerLine"></div>
    </div>
  );
};
