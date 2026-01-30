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

package org.apache.geaflow.ai.client;

import com.google.gson.Gson;
import java.io.*;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.common.model.ChatService;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.graph.io.*;
import org.noear.solon.core.PropsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowMemoryClientCLI {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowMemoryClientCLI.class);

    private static final String BASE_URL = "http://localhost:8080";
    private static final String SERVER_URL = BASE_URL + "/api/test";
    private static final String CREATE_URL = BASE_URL + "/graph/create";
    private static final String SCHEMA_URL = BASE_URL + "/graph/addEntitySchema";
    private static final String INSERT_URL = BASE_URL + "/graph/insertEntity";
    private static final String CONTEXT_URL = BASE_URL + "/query/context";
    private static final String EXEC_URL = BASE_URL + "/query/exec";

    private static final String DEFAULT_GRAPH_NAME = "memory_graph";
    private static final String VERTEX_LABEL = "chunk";
    private static final String EDGE_LABEL = "relation";
    private final Scanner scanner = new Scanner(System.in);
    private final Gson gson = new Gson();
    private String currentGraphName = DEFAULT_GRAPH_NAME;
    private String currentSessionId = null;
    private static ModelConfig chatModelConfig = new ModelConfig();
    private ChatService chatService = null;

    public static String calculateMD5(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(text.getBytes(StandardCharsets.UTF_8));
            BigInteger no = new BigInteger(1, messageDigest);
            StringBuilder hashText = new StringBuilder(no.toString(16));
            while (hashText.length() < 32) {
                hashText.insert(0, "0");
            }
            return hashText.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        GeaFlowMemoryClientCLI client = new GeaFlowMemoryClientCLI();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                System.out.printf("arg[%d]: %s\n", i, args[i]);
            }
        }
        String externalConfigPath = args != null ? args[0] : "/etc/geaflow_memory.properties";
        if (StringUtils.isNotBlank(externalConfigPath)) {
            java.nio.file.Path configFile = Paths.get(externalConfigPath);
            if (Files.exists(configFile)) {
                try {
                    System.out.print("Loading external config from: " + externalConfigPath);
                    LOGGER.info("Loading external config from: {}", externalConfigPath);
                    File file = configFile.toFile();
                    URL configUrl = file.toURI().toURL();
                    Properties props = new PropsLoader().load(configUrl);
                    String model = props.getProperty("model.chat.name", null);
                    String url = props.getProperty("model.chat.url", null);
                    String api = props.getProperty("model.chat.api", null);
                    String token = props.getProperty("model.chat.token", null);
                    chatModelConfig = new ModelConfig(model, url, api, token);
                } catch (Exception e) {
                    System.out.print("Failed to load external config " + e.getMessage());
                    LOGGER.warn("Failed to load external config '{}': {}. Proceeding with defaults.",
                        externalConfigPath, e.getMessage());
                }
            } else {
                System.out.print("External config file not found: '" + externalConfigPath + "'. Using default/internal configuration.");
                LOGGER.warn("External config file not found: '{}'. Using default/internal configuration.",
                    externalConfigPath);
            }
        } else {
            System.out.print("No external config.path specified. Using embedded configuration.");
            LOGGER.info("No external config.path specified. Using embedded configuration.");
        }

        client.start();
    }

    public void start() {
        printWelcome();

        while (true) {
            try {
                System.out.print("\ngeaflow> ");
                String input = scanner.nextLine().trim();

                if (input.isEmpty()) {
                    continue;
                }

                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                    System.out.println("Goodbye!");
                    break;
                }

                if (input.equalsIgnoreCase("help")) {
                    printHelp();
                    continue;
                }

                processCommand(input);

            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                if (e.getCause() != null) {
                    System.err.println("Cause: " + e.getCause().getMessage());
                }
            }
        }

        scanner.close();
    }

    private void processCommand(String command) throws IOException {
        String[] parts = command.split("\\s+", 2);
        String cmd = parts[0].toLowerCase();
        String param = parts.length > 1 ? parts[1] : "";

        switch (cmd) {
            case "test":
                testServer();
                break;

            case "use":
                currentGraphName = param.isEmpty() ? DEFAULT_GRAPH_NAME : param;
                break;

            case "create":
                String graphName = param.isEmpty() ? DEFAULT_GRAPH_NAME : param;
                createGraph(graphName);
                currentGraphName = graphName;
                break;

            case "remember":
                if (param.isEmpty()) {
                    System.out.println("Please enter content to remember:");
                    param = scanner.nextLine();
                }
                rememberContent(param);
                break;

            case "query":
                if (param.isEmpty()) {
                    System.out.println("Please enter your query:");
                    param = scanner.nextLine();
                }
                executeQuery(param);
                break;

            default:
                System.out.println("Unknown command: " + cmd);
                System.out.println("Available commands: test, create, use, remember, query, help, exit");
        }
    }

    private void testServer() throws IOException {
        System.out.println("Testing server connection...");
        String response = sendGetRequest(SERVER_URL);
        System.out.println("✓ Server response: " + response);
    }

    private void createGraph(String graphName) throws IOException {
        System.out.println("Creating graph: " + graphName);

        GraphSchema testGraph = new GraphSchema();
        testGraph.setName(graphName);
        String graphJson = gson.toJson(testGraph);
        String response = sendPostRequest(CREATE_URL, graphJson);
        System.out.println("✓ Graph created: " + response);

        Map<String, String> params = new HashMap<>();
        params.put("graphName", graphName);
        VertexSchema vertexSchema = new VertexSchema(VERTEX_LABEL, Constants.PREFIX_ID,
            Collections.singletonList("text"));
        response = sendPostRequest(SCHEMA_URL, gson.toJson(vertexSchema), params);
        System.out.println("✓ Chunk schema added: " + response);

        EdgeSchema edgeSchema = new EdgeSchema(EDGE_LABEL, Constants.PREFIX_SRC_ID, Constants.PREFIX_DST_ID,
            Collections.singletonList("rel"));
        response = sendPostRequest(SCHEMA_URL, gson.toJson(edgeSchema), params);
        System.out.println("✓ Relation schema added: " + response);

        System.out.println("✓ Graph '" + graphName + "' is ready for use!");
    }

    private void rememberContent(String content) throws IOException {
        if (currentGraphName == null) {
            System.out.println("No graph selected. Please create a graph first.");
            return;
        }

        if (content.trim().toLowerCase(Locale.ROOT).startsWith("doc")) {
            String path = content.trim().substring(3).trim();

            TextFileReader textFileReader = new TextFileReader(10000);
            textFileReader.readFile(path);
            List<String> chunks = IntStream.range(0, textFileReader.getRowCount())
                .mapToObj(textFileReader::getRow)
                .map(String::trim).collect(Collectors.toList());
            for (String chunk : chunks) {
                String response = rememberChunk(chunk);
                System.out.println("✓ Content remembered: " + response);
            }
        }

        System.out.println("Remembering content...");
        String response = rememberChunk(content);
        System.out.println("✓ Content remembered: " + response);

    }

    private String rememberChunk(String content) throws IOException {
        String vertexId = calculateMD5(content);
        Vertex chunkVertex = new Vertex("chunk", vertexId, Collections.singletonList(content));
        String vertexJson = gson.toJson(chunkVertex);

        Map<String, String> params = new HashMap<>();
        params.put("graphName", currentGraphName);

        return sendPostRequest(INSERT_URL, vertexJson, params);
    }

    private void executeQuery(String query) throws IOException {
        if (currentGraphName == null) {
            System.out.println("No graph selected. Please create a graph first.");
            return;
        }

        System.out.println("Creating new session...");
        Map<String, String> params = new HashMap<>();
        params.put("graphName", currentGraphName);
        String response = sendPostRequest(CONTEXT_URL, "", params);
        currentSessionId = response.trim();
        System.out.println("✓ Session created: " + currentSessionId);

        System.out.println("Executing query: " + query);

        params = new HashMap<>();
        params.put("sessionId", currentSessionId);

        response = sendPostRequest(EXEC_URL, query, params);
        System.out.println("✓ Search result:");
        System.out.println("========================");
        System.out.println(response);
        System.out.println("========================");
        String modelResponseWithRag = getChatService().chat(query + "\n[\n" + response + "]");
        System.out.println("✓ Query result:");
        System.out.println("========================");
        System.out.println(modelResponseWithRag);
        System.out.println("========================");
    }

    public ChatService getChatService() {
        if (chatService == null) {
            chatService = new ChatService(chatModelConfig);
        }
        return chatService;
    }

    private String sendGetRequest(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("HTTP error code: " + responseCode);
        }

        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    private String sendPostRequest(String urlStr, String body) throws IOException {
        return sendPostRequest(urlStr, body, Collections.emptyMap());
    }

    private String sendPostRequest(String urlStr, String body, Map<String, String> queryParams) throws IOException {
        if (!queryParams.isEmpty()) {
            StringBuilder urlBuilder = new StringBuilder(urlStr);
            urlBuilder.append("?");
            boolean first = true;
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (!first) {
                    urlBuilder.append("&");
                }
                urlBuilder.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                urlBuilder.append("=");
                urlBuilder.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                first = false;
            }
            urlStr = urlBuilder.toString();
        }

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = body.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            String errorMessage = readErrorResponse(conn);
            throw new IOException("HTTP error code: " + responseCode + ", Message: " + errorMessage);
        }

        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine);
            }
            return response.toString();
        }
    }

    private String readErrorResponse(HttpURLConnection conn) throws IOException {
        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine);
            }
            return response.toString();
        } catch (Exception e) {
            return "No error message available";
        }
    }

    private void printWelcome() {
        System.out.println("=========================================");
        System.out.println("  GeaFlow Memory Server - Simple Client");
        System.out.println("=========================================");
        System.out.println("Simple Commands:");
        System.out.println("  test               - Test server connection");
        System.out.println("  create [name]      - Create a new memory graph");
        System.out.println("  use [name]         - Use a new memory graph");
        System.out.println("  remember <content> - Store content to memory");
        System.out.println("  query <question>   - Ask questions about memory");
        System.out.println("  help               - Show this help");
        System.out.println("  exit               - Quit the client");
        System.out.println("=========================================");
        System.out.println("Default graph name: " + DEFAULT_GRAPH_NAME);
        System.out.println("Server URL: " + BASE_URL);
        System.out.println("=========================================");
    }

    private void printHelp() {
        System.out.println("\nAvailable Commands:");
        System.out.println("-------------------");
        System.out.println("test");
        System.out.println("  Test if the GeaFlow server is running");
        System.out.println("  Example: test");
        System.out.println();
        System.out.println("create [graph_name]");
        System.out.println("  Create a new memory graph with default schema");
        System.out.println("  Creates: chunk vertices and relation edges");
        System.out.println("  Default name: " + DEFAULT_GRAPH_NAME);
        System.out.println("  Example: create");
        System.out.println("  Example: create my_memory");
        System.out.println();
        System.out.println("use [graph_name]");
        System.out.println("  Use a new memory graph");
        System.out.println("  Default name: " + DEFAULT_GRAPH_NAME);
        System.out.println("  Example: use my_memory");
        System.out.println();
        System.out.println("remember <content>");
        System.out.println("  Store text content into memory");
        System.out.println("  Creates a 'chunk' vertex with the content");
        System.out.println("  Example: remember \"孔子是中国古代的思想家\"");
        System.out.println("  Example: remember");
        System.out.println("    (will prompt for content)");
        System.out.println();
        System.out.println("query <question>");
        System.out.println("  Query the memory with natural language");
        System.out.println("  Example: query \"Who is Confucius?\"");
        System.out.println("  Example: query");
        System.out.println("    (will prompt for question)");
        System.out.println();
        System.out.println("exit / quit");
        System.out.println("  Exit the client");
    }
}
