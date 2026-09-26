# Week 1 Retrieval API

Build the memory server; run its `GeaFlowMemoryServer` main class with the project runtime classpath:

```bash
GEAFLOW_SERVER_PORT=8080 mvn -pl geaflow-ai -DskipTests package
```

Create a small keyword graph and load one vertex:

```bash
curl -sS -X POST http://localhost:8080/graph/create \
  -H 'Content-Type: application/json' \
  -d '{"graphName":"Confucius","vertexSchemaList":[],"edgeSchemaList":[]}'

curl -sS -X POST 'http://localhost:8080/graph/addEntitySchema?graphName=Confucius' \
  -H 'Content-Type: application/json' \
  -d '{"label":"chunk","idField":"id","fields":["text"]}'

curl -sS -X POST 'http://localhost:8080/graph/insertEntity?graphName=Confucius' \
  -H 'Content-Type: application/json' \
  -d '{"label":"chunk","id":"confucius-1","values":["Confucius taught ethics."]}'
```

Run bounded keyword retrieval without creating a session:

```bash
curl -sS -X POST http://localhost:8080/api/v1/retrievals \
  -H 'Content-Type: application/json' \
  -H 'X-Request-Id: curl-example-1' \
  -d '{"graphName":"Confucius","query":"Confucius","mode":"KEYWORD",\
"budget":{"topK":10,"timeoutMs":3000,"maxCandidates":100,"tokenBudget":4096}}'
```

Health and readiness are separate checks:

```bash
curl -i http://localhost:8080/health
curl -i http://localhost:8080/ready
curl -sS http://localhost:8080/metrics/retrieval
```
