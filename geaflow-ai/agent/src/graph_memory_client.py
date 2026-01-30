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

import json
import requests
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from pathlib import Path
import configparser
import logging


@dataclass
class VertexSchema:
    label: str
    fields: List[str]
    idField: str


@dataclass
class EdgeSchema:
    label: str
    fields: List[str]
    srcIdField: str
    dstIdField: str


@dataclass
class GraphSchema:
    graphName: str = "graph"
    vertexSchemaList: List[VertexSchema] = field(default_factory=list)
    edgeSchemaList: List[EdgeSchema] = field(default_factory=list)
    promptFormatter: Optional[object] = None


@dataclass
class Vertex:
    id: str
    label: str
    values: List[str]


@dataclass
class Edge:
    srcId: str
    dstId: str
    label: str
    values: List[str]


@dataclass
class ModelConfig:
    model: Optional[str] = None
    url: Optional[str] = None
    api: Optional[str] = None
    token: Optional[str] = None


class ChatService:
    def __init__(self, config: ModelConfig):
        self.config = config

    def chat(self, query: str) -> str:
        return f"AI Response for: {query}"


class TextFileReader:
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.rows = []

    def read_file(self, file_path: str) -> None:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.rows = [line.strip() for line in content.split('\n') if line.strip()]
        except Exception as e:
            logging.error(f"Failed to read file {file_path}: {e}")
            raise

    def get_row_count(self) -> int:
        return len(self.rows)

    def get_row(self, index: int) -> str:
        if 0 <= index < len(self.rows):
            return self.rows[index]
        raise IndexError(f"Row index {index} out of range")


class GeaFlowMemoryClientCLI:
    def __init__(self):
        self.base_url = "http://localhost:8080"
        self.server_url = f"{self.base_url}/api/test"
        self.create_url = f"{self.base_url}/graph/create"
        self.schema_url = f"{self.base_url}/graph/addEntitySchema"
        self.insert_url = f"{self.base_url}/graph/insertEntity"
        self.context_url = f"{self.base_url}/query/context"
        self.exec_url = f"{self.base_url}/query/exec"

        self.default_graph_name = "memory_graph"
        self.vertex_label = "chunk"
        self.edge_label = "relation"

        self.current_graph_name = self.default_graph_name
        self.current_session_id = None
        self.chat_model_config = ModelConfig()
        self.chat_service = None

        self.PREFIX_GRAPH = "graph"
        self.PREFIX_ID = "id"
        self.PREFIX_SRC_ID = "srcId"
        self.PREFIX_DST_ID = "dstId"

    def load_config(self, config_path: str) -> None:
        if not config_path:
            print("No external config path specified. Using embedded configuration.")
            logging.info("No external config path specified. Using embedded configuration.")
            return

        config_file = Path(config_path)
        if config_file.exists():
            try:
                print(f"Loading external config from: {config_path}")
                logging.info(f"Loading external config from: {config_path}")

                config = configparser.ConfigParser()
                config.read(config_path)

                self.chat_model_config.model = config.get('DEFAULT', 'model.chat.name', fallback=None)
                self.chat_model_config.url = config.get('DEFAULT', 'model.chat.url', fallback=None)
                self.chat_model_config.api = config.get('DEFAULT', 'model.chat.api', fallback=None)
                self.chat_model_config.token = config.get('DEFAULT', 'model.chat.token', fallback=None)

            except Exception as e:
                print(f"Failed to load external config {e}")
                logging.warning(f"Failed to load external config '{config_path}': {e}. Proceeding with defaults.")
        else:
            print(f"External config file not found: '{config_path}'. Using default/internal configuration.")
            logging.warning(f"External config file not found: '{config_path}'. Using default/internal configuration.")

    def start(self):
        self.print_welcome()

        while True:
            try:
                user_input = input("\ngeaflow> ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ["exit", "quit"]:
                    print("Goodbye!")
                    break

                if user_input.lower() == "help":
                    self.print_help()
                    continue

                self.process_command(user_input)

            except Exception as e:
                print(f"Error: {e}")
                if hasattr(e, '__cause__') and e.__cause__:
                    print(f"Cause: {e.__cause__}")

    def process_command(self, command: str):
        parts = command.split(maxsplit=1)
        cmd = parts[0].lower()
        param = parts[1] if len(parts) > 1 else ""

        if cmd == "test":
            self.test_server()
        elif cmd == "use":
            self.current_graph_name = param if param else self.default_graph_name
            print(f"Using graph: {self.current_graph_name}")
        elif cmd == "create":
            graph_name = param if param else self.default_graph_name
            self.create_graph(graph_name)
            self.current_graph_name = graph_name
        elif cmd == "remember":
            if not param:
                print("Please enter content to remember:")
                param = input().strip()
            self.remember_content(param)
        elif cmd == "query":
            if not param:
                print("Please enter your query:")
                param = input().strip()
            self.execute_query(param)
        else:
            print(f"Unknown command: {cmd}")
            print("Available commands: test, create, use, remember, query, help, exit")

    def test_server(self):
        print("Testing server connection...")
        response = self.send_get_request(self.server_url)
        print(f"✓ Server response: {response}")

    def create_graph(self, graph_name: str):
        print(f"Creating graph: {graph_name}")

        graph_schema = GraphSchema(graphName=graph_name)
        graph_json = json.dumps(graph_schema.__dict__)
        print(f"✓ graph_json: {graph_json}")
        params = {"graphName": graph_name}
        response = self.send_post_request(self.create_url, graph_json, params)
        print(f"✓ Graph created: {response}")

        vertex_schema = VertexSchema(
            label=self.vertex_label,
            fields=["text"],
            idField=self.PREFIX_ID
        )
        vertex_schema_json = json.dumps(vertex_schema.__dict__)
        print(f"✓ vertex_schema_json: {vertex_schema_json}")
        response = self.send_post_request(
            self.schema_url,
            vertex_schema_json,
            params
        )
        print(f"✓ Chunk schema added: {response}")

        edge_schema = EdgeSchema(
            label=self.edge_label,
            fields=["rel"],
            srcIdField=self.PREFIX_SRC_ID,
            dstIdField=self.PREFIX_DST_ID
        )
        edge_schema_json = json.dumps(edge_schema.__dict__)
        print(f"✓ edge_schema_json: {edge_schema_json}")
        response = self.send_post_request(
            self.schema_url,
            edge_schema_json,
            params
        )
        print(f"✓ Relation schema added: {response}")
        self.current_graph_name = graph_name
        print(f"✓ Graph '{graph_name}' is ready for use!")

    def remember_content(self, content: str):
        if not self.current_graph_name:
            print("No graph selected. Please create a graph first.")
            return

        if content.strip().lower().startswith("doc"):
            path = content.strip()[3:].strip()
            self.remember_document(path)
        else:
            print("Remembering content...")
            response = self.remember_chunk(content)
            print(f"✓ Content remembered: {response}")

    def remember_document(self, file_path: str):
        try:
            text_file_reader = TextFileReader(10000)
            text_file_reader.read_file(file_path)

            for i in range(text_file_reader.get_row_count()):
                chunk = text_file_reader.get_row(i)
                response = self.remember_chunk(chunk)
                print(f"✓ Content remembered: {response}")
        except Exception as e:
            print(f"Failed to read document: {e}")

    def remember_chunk(self, content: str) -> str:
        vertex_id = str(hash(content))
        chunk_vertex = Vertex(
            id=vertex_id,
            label=self.vertex_label,
            values=[content]
        )
        vertex_json = json.dumps(chunk_vertex.__dict__)
        print(f"✓ graphName: {self.current_graph_name} remember_vertex_json: {vertex_json}")
        params = {"graphName": self.current_graph_name}
        return self.send_post_request(self.insert_url, vertex_json, params)

    def execute_query(self, query: str):
        if not self.current_graph_name:
            print("No graph selected. Please create a graph first.")
            return

        print("Creating new session...")
        params = {"graphName": self.current_graph_name}
        response = self.send_post_request(self.context_url, "", params)
        self.current_session_id = response.strip()
        print(f"✓ Session created: {self.current_session_id}")

        print(f"GraphName: {self.current_graph_name} Executing query: {query}")
        params = {"sessionId": self.current_session_id}
        response = self.send_post_request(self.exec_url, query, params)

        print("✓ Search result:")
        print("========================")
        print(response)
        print("========================")

        model_response_with_rag = self.get_chat_service().chat(query + "\n[\n" + response + "]")
        print("✓ Query result:")
        print("========================")
        print(model_response_with_rag)
        print("========================")
        return response

    def get_chat_service(self) -> ChatService:
        if self.chat_service is None:
            self.chat_service = ChatService(self.chat_model_config)
        return self.chat_service

    def send_get_request(self, url: str) -> str:
        try:
            response = requests.get(url, headers={"Accept": "application/json"})
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise Exception(f"HTTP request failed: {e}")

    def send_post_request(self, url: str, body: str, params: Optional[Dict] = None) -> str:
        try:
            response = requests.post(
                url,
                params=params,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise Exception(f"HTTP request failed: {e}")

    def print_welcome(self):
        print("=========================================")
        print("  GeaFlow Memory Server - Simple Client")
        print("=========================================")
        print("Simple Commands:")
        print("  test               - Test server connection")
        print("  create [name]      - Create a new memory graph")
        print("  use [name]         - Use a new memory graph")
        print("  remember <content> - Store content to memory")
        print("  query <question>   - Ask questions about memory")
        print("  help               - Show this help")
        print("  exit               - Quit the client")
        print("=========================================")
        print(f"Default graph name: {self.default_graph_name}")
        print(f"Server URL: {self.base_url}")
        print("=========================================")

    def print_help(self):
        print("\nAvailable Commands:")
        print("-------------------")
        print("test")
        print("  Test if the GeaFlow server is running")
        print("  Example: test")
        print()
        print("create [graph_name]")
        print("  Create a new memory graph with default schema")
        print("  Creates: chunk vertices and relation edges")
        print(f"  Default name: {self.default_graph_name}")
        print("  Example: create")
        print("  Example: create my_memory")
        print()
        print("use [graph_name]")
        print("  Use a new memory graph")
        print(f"  Default name: {self.default_graph_name}")
        print("  Example: use my_memory")
        print()
        print("remember <content>")
        print("  Store text content into memory")
        print('  Example: remember "孔子是中国古代的思想家"')
        print("  Example: remember")
        print("    (will prompt for content)")
        print()
        print("query <question>")
        print("  Query the memory with natural language")
        print('  Example: query "Who is Confucius?"')
        print("  Example: query")
        print("    (will prompt for question)")
        print()
        print("exit / quit")
        print("  Exit the client")
