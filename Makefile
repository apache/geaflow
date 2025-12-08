#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SHELL:=/bin/bash

# Log the running target
LOG_TARGET = echo -e "\033[0;32m==================> Running $@ ============> ... \033[0m"

.PHONY: all
all: build

.PHONY: build
build: ## Build the project
	@$(LOG_TARGET)
	mvn -B -e clean package -DskipTests -Duser.timezone=Asia/Shanghai

.PHONY: test
test: ## Run tests on JDK 8
	@$(LOG_TARGET)
	mvn -B -e clean test -Pjdk8 -Duser.timezone=Asia/Shanghai -Dlog4j.configuration="log4j.rootLogger=WARN, stdout"

.PHONY: test-jdk11
test-jdk11: ## Run tests on JDK 11 (excludes Hive connector)
	@$(LOG_TARGET)
	mvn -B -e clean test -Pjdk11 -pl "!geaflow/geaflow-dsl/geaflow-dsl-connector/geaflow-dsl-connector-hive,!geaflow/geaflow-dsl/geaflow-dsl-connector-tests" -Duser.timezone=Asia/Shanghai -Dlog4j.configuration="log4j.rootLogger=WARN, stdout"

.PHONY: checkstyle
checkstyle: ## Run checkstyle
	@$(LOG_TARGET)
	mvn checkstyle:check

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
