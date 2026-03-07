/******************************************/
/*   TableName = geaflow_audit   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_audit` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
  `tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
  `guid` char(64) NOT NULL COMMENT 'ID',
  `creator_id` char(64) NOT NULL COMMENT 'Creator ID',
  `modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
  `resource_id` char(64) NOT NULL COMMENT 'Resource ID',
  `resource_type` varchar(64) NOT NULL COMMENT 'Resource Type',
  `operation_type` varchar(64) NOT NULL COMMENT 'Operation',
  `detail` text DEFAULT NULL COMMENT 'Detail',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_resource` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Audit Table'
;

/******************************************/
/*   TableName = geaflow_authorization   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_authorization` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`user_id` char(64) NOT NULL COMMENT 'User ID',
`authority_type` char(32) NOT NULL COMMENT 'Authority Type',
`resource_type` char(32) NOT NULL COMMENT 'Resource Type',
`resource_id` char(64) NOT NULL COMMENT 'Resource ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_resource` (`tenant_id`, `user_id`, `authority_type`, `resource_type`, `resource_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Authorization Table'
;

/******************************************/
/*   TableName = geaflow_cluster   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_cluster` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`type` char(32) NOT NULL COMMENT 'Cluster Type',
`config` mediumtext NOT NULL COMMENT 'Cluster Config',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Cluster Table'
;

/******************************************/
/*   TableName = geaflow_edge   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_edge` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Edge Table'
;

/******************************************/
/*   TableName = geaflow_field   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_field` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`type` varchar(32) NOT NULL COMMENT 'Field Type',
`category` varchar(1024) NOT NULL COMMENT 'Field Category',
`resource_id` char(64) NOT NULL COMMENT 'Resource ID',
`resource_type` varchar(32) NOT NULL COMMENT 'Resource Type',
`sort_key` int(10) unsigned NOT NULL COMMENT 'Sort Key',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`resource_id`, `resource_type`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Field Table'
;

/******************************************/
/*   TableName = geaflow_function   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_function` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'JAR ID',
`entry_class` varchar(256) DEFAULT NULL COMMENT 'Entry Class',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Function Table'
;

/******************************************/
/*   TableName = geaflow_graph   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_graph` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`plugin_config_id` char(64) NOT NULL COMMENT 'Plugin Config ID',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Graph Table'
;

/******************************************/
/*   TableName = geaflow_graph_struct_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_graph_struct_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`graph_id` char(64) NOT NULL COMMENT 'Graph ID',
`resource_id` char(64) NOT NULL COMMENT 'Resource ID',
`resource_type` varchar(32) NOT NULL COMMENT 'Resource Type',
`sort_key` int(10) unsigned NOT NULL COMMENT 'Sort Key',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`graph_id`, `resource_id`, `resource_type`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Graph Resource Mapping Table'
;

/******************************************/
/*   TableName = geaflow_instance   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_instance` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Create ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Instance Table'
;

/******************************************/
/*   TableName = geaflow_job   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_job` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
`type` varchar(32) NOT NULL COMMENT 'Task Type',
`user_code` text DEFAULT NULL COMMENT 'User Code',
`struct_mappings` text DEFAULT NULL COMMENT 'Struct Mapping',
`sla_id` char(64) DEFAULT NULL COMMENT 'SLA ID',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'JAR ID',
`entry_class` varchar(256) DEFAULT NULL COMMENT 'Entry Class',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Task Table'
;

/******************************************/
/*   TableName = geaflow_job_resource_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_job_resource_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`job_id` char(64) NOT NULL COMMENT 'Job ID',
`resource_name` varchar(128) NOT NULL COMMENT 'Resource ID',
`resource_type` varchar(32) NOT NULL COMMENT 'Resource Type',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`job_id`, `resource_name`, `resource_type`, `instance_id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Job Resource Mapping Table'
;

/******************************************/
/*   TableName = geaflow_plugin   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_plugin` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`plugin_type` varchar(32) NOT NULL COMMENT 'Plugin Type',
`plugin_category` varchar(32) NOT NULL COMMENT 'Plugin Category',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'JAR ID',
`system` tinyint(4) DEFAULT NULL COMMENT 'System Level',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Plugin Table'
;

/******************************************/
/*   TableName = geaflow_plugin_config   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_plugin_config` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`type` varchar(32) NOT NULL COMMENT 'Plugin Type',
`config` text DEFAULT NULL COMMENT 'Plugin Config',
`category` varchar(32) NOT NULL COMMENT 'Plugin Category',
`system` tinyint(4) DEFAULT NULL COMMENT 'System Level',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Plugin Config Table'
;

/******************************************/
/*   TableName = geaflow_release   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_release` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`job_id` char(64) NOT NULL COMMENT 'Job ID',
`cluster_id` char(64) NOT NULL COMMENT 'Cluster ID',
`job_plan` text NOT NULL COMMENT 'Job Plan',
`job_config` text NOT NULL COMMENT 'Job User Config',
`cluster_config` text NOT NULL COMMENT 'Job Cluster Config',
`version_id` char(64) NOT NULL COMMENT 'Version ID',
`version` int(10) unsigned NOT NULL COMMENT 'Release Version',
`url` varchar(1024) NOT NULL COMMENT 'Package URL',
`md5` varchar(32) NOT NULL COMMENT 'Package MD5',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Release Table'
;

/******************************************/
/*   TableName = geaflow_remote_file   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_remote_file` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`type` varchar(32) NOT NULL COMMENT 'File Type',
`url` varchar(1024) NOT NULL COMMENT 'File URL',
`md5` char(32) NOT NULL COMMENT 'File MD5',
`system` tinyint(4) NOT NULL COMMENT 'System Level',
`path` varchar(1024) NOT NULL COMMENT 'File Path',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Remote File Table'
;

/******************************************/
/*   TableName = geaflow_system_config   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_system_config` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Create ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`value` mediumtext DEFAULT NULL COMMENT 'Config Value',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'System Config Table'
;

/******************************************/
/*   TableName = geaflow_table   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_table` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`plugin_config_id` char(64) NOT NULL COMMENT 'Plugin Config ID',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Table Table'
;

/******************************************/
/*   TableName = geaflow_task   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_task` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`release_id` char(64) NOT NULL COMMENT 'Release ID',
`start_time` timestamp NULL DEFAULT NULL COMMENT 'Start Time',
`end_time` timestamp NULL DEFAULT NULL COMMENT 'End Time',
`type` varchar(32) NOT NULL COMMENT 'Task Type',
`status` varchar(32) NOT NULL COMMENT 'Task Status',
`runtime_meta_config_id` char(64) NOT NULL COMMENT 'Runtime Meta Config ID',
`ha_meta_config_id` char(64) NOT NULL COMMENT 'HA Meta Config ID',
`metric_config_id` char(64) NOT NULL COMMENT 'Metric Config ID',
`data_config_id` char(64) NOT NULL COMMENT 'Data Config ID',
`job_id` char(64) NOT NULL COMMENT 'Job ID',
`token` char(64) DEFAULT NULL COMMENT 'Task Token',
`host` varchar(256) DEFAULT NULL COMMENT 'Schedule Host',
`handle` text DEFAULT NULL COMMENT 'Task Handle',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Task Table'
;

/******************************************/
/*   TableName = geaflow_tenant   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_tenant` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Create ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`quota_id` char(64) DEFAULT NULL COMMENT 'Quota ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Tenant Table'
;

/******************************************/
/*   TableName = geaflow_tenant_user_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_tenant_user_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Create ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`user_id` char(64) NOT NULL COMMENT 'User ID',
`active` tinyint(4) NOT NULL COMMENT 'Active Tenant',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `user_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Tenant User Mapping Table'
;

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
/*   TableName = geaflow_user   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_user` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) DEFAULT NULL COMMENT 'Creator ID',
`modifier_id` char(64) DEFAULT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`password_sign` varchar(128) DEFAULT NULL COMMENT 'Password Signature',
`phone` char(32) DEFAULT NULL COMMENT 'Phone',
`email` char(128) DEFAULT NULL COMMENT 'Email',
`session_token` char(64) DEFAULT NULL COMMENT 'Session Token',
`system_session` tinyint(4) DEFAULT NULL COMMENT 'System Session',
`access_time` timestamp NULL DEFAULT NULL COMMENT 'Last Access Time',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`name`),
UNIQUE KEY `uk_session_token` (`session_token`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'User Table'
;

/******************************************/
/*   TableName = geaflow_user_role_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_user_role_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`user_id` char(64) NOT NULL COMMENT 'User ID',
`role_type` char(32) NOT NULL COMMENT 'Role Type',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_mapping` (`tenant_id`, `user_id`, `role_type`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'User Role Mapping Table'
;

/******************************************/
/*   TableName = geaflow_version   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_version` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) DEFAULT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`engine_jar_id` char(64) DEFAULT NULL COMMENT 'Engine JAR',
`lang_jar_id` char(64) DEFAULT NULL COMMENT 'Language JAR',
`publish` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT 'Published',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Version Table'
;

/******************************************/
/*   TableName = geaflow_vertex   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_vertex` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`name` varchar(128) NOT NULL COMMENT 'Name',
`comment` varchar(1024) DEFAULT NULL COMMENT 'Comment',
`instance_id` char(64) NOT NULL COMMENT 'Instance ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Vertex Table'
;

CREATE TABLE `geaflow_endpoint` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify Time',
`creator_id` char(64) NOT NULL COMMENT 'Creator ID',
`modifier_id` char(64) NOT NULL COMMENT 'Modifier ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`tenant_id` char(64) NOT NULL COMMENT 'Tenant ID',
`edge_id` char(64) NOT NULL COMMENT 'Edge Id',
`source_id` char(64) NOT NULL COMMENT 'Source Id',
`target_id` char(64) NOT NULL COMMENT 'Target Id',
`graph_id` char(64) NOT NULL COMMENT 'Graph ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_src_target` (`edge_id`, `source_id`, `target_id`, `graph_id`),
KEY `idx_edge_id` (`edge_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Endpoint Table'
;

CREATE TABLE `geaflow_statement` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP comment '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment '修改时间',
`tenant_id` char(64) NOT NULL comment '租户ID',
`guid` char(64) NOT NULL comment 'ID',
`creator_id` char(64) NOT NULL comment '创建人ID',
`modifier_id` char(64) NOT NULL comment '修改人ID',
`job_id` char(64) NOT NULL comment '作业Id',
`script` text NOT NULL comment '查询语句',
`status` varchar(64) DEFAULT NULL comment '状态',
`result` mediumtext DEFAULT NULL comment '查询结果',
PRIMARY KEY(`id`),
UNIQUE KEY `uk_guid`(`guid`),
KEY `idx_job_id`(`job_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Statemnet Table';

CREATE TABLE `geaflow_llm` (
`id` bigint unsigned NOT NULL AUTO_INCREMENT,
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
`tenant_id` char(64) DEFAULT NULL,
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT 'id',
`modifier_id` char(64) NOT NULL COMMENT 'id',
`name` varchar(128) NOT NULL,
`comment` varchar(512) DEFAULT NULL,
`url` varchar(512) NOT NULL,
`type` varchar(32) NOT NULL,
`args` text DEFAULT NULL COMMENT 'args',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Language Models';

CREATE TABLE `geaflow_chat` (
`id` bigint unsigned NOT NULL AUTO_INCREMENT,
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
`tenant_id` char(64) DEFAULT NULL,
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT 'id',
`modifier_id` char(64) NOT NULL COMMENT 'id',
`prompt` text NOT NULL,
`answer` text DEFAULT NULL,
`model_id` char(64) NOT NULL COMMENT 'modelId',
`job_id` char(64) DEFAULT NULL COMMENT 'jobId',
`status` varchar(32) DEFAULT NULL COMMENT 'stauts',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'Chat Records';
