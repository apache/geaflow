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

import argparse
import logging

from src.graph_memory_client import GeaFlowMemoryClientCLI


def main():
    parser = argparse.ArgumentParser(description='GeaFlow Memory Client CLI')
    parser.add_argument('config_path', nargs='?', default='/etc/geaflow_memory.properties',
                        help='Path to configuration file')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    client = GeaFlowMemoryClientCLI()

    client.load_config(args.config_path)

    client.start()


if __name__ == "__main__":
    main()
