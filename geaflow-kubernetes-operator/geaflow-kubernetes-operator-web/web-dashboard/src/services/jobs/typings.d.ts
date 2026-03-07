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

// @ts-ignore
/* eslint-disable */

declare namespace API {

  type CurrentUser = {
    name?: string
    avatar?: string
  }

  type GeaflowJob = {
    apiVersion?: string
    kind?: string
    metadata?: {
      creationTimestamp?: string
      generation?: number
      name?: string
      namespace?: string
      resourceVersion?: number
      uid?: string
    }
    spec?: {
      clientSpec?: {
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      containerSpec?: {
        containerNum?: number
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
        workerNumPerContainer?: number
      }
      driverSpec?: {
        driverNum?: number
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      engineJars?: [
        {
          md5?: string
          name?: string
          url?: string
        }
      ]
      entryClass?: string
      image?: string
      imagePullPolicy?: string
      masterSpec?: {
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      serviceAccount?: string
      udfJars?: [
        {
          md5?: string
          name?: string
          url?: string
        }
      ]
      userSpec?: {
        additionalArgs?: {}
        metricConfig?: {}
        stateConfig?: {}
      }
    }
    status?: {
      clientState?: string
      jobUid?: number
      lastReconciledSpec?: string
      masterState?: string
      state?: string
      errorMessage?: string
    }
  }

  type ClusterOverview = {
    host?: string
    namespace?: string
    masterUrl?: string
    totalJobNum?: number
    jobStateNumMap?: Record<string, number | undefined>
  }

}
