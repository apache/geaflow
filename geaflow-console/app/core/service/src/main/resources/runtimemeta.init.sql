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

/******************************************/
/*   TableName = backend_meta   */
/******************************************/
CREATE TABLE IF NOT EXISTS `backend_meta` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
  `pk` varchar(255) NOT NULL COMMENT 'Meta key',
  `value` mediumblob DEFAULT NULL COMMENT 'Meta value',
  `gmt_create` timestamp NULL DEFAULT NULL COMMENT 'Create Time',
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Modify Time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pk` (`pk`),
  KEY `idx_gmt_modified` (`gmt_modified`)
)  DEFAULT CHARSET = utf8mb4 COMMENT = 'GeaFlow Task Runtime Meta'
;
